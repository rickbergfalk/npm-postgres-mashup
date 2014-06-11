// READER BEWARE!
// I indent with tabs, use semicolons (most of the time), and wrote this on windows.

var fs = require('fs');
var follow = require('follow');
var knex = require('knex');
var async = require('async');
var postgrator = require('postgrator');


/*     Variables for Later
============================================================================= */
var changeCount = 0;              // every time we begin to process a change, this will be incremented
var errorLimit = 20;              // If this many errors happen, we'll stop persisting to postgres and quit
var errorCount = 0;
var couchUrl;
var postgresHost;
var postgresUser;
var postgresPassword;
var postgresDatabase;
var beNoisy = false;              // if set to true we'll console.log progress
var logFile = __dirname + "/error-log.txt";
var onCatchup;

var schemaversion = '004';

var feed; // follow feed
var startingSeq = 1;
var emptyPostgres = false;

var parallelLimit = 10;    // number of changes we'll process at once
var changesProcessing = 0; // used to track number of changes that are currently processing


/*  Manage Flow
    stops/starts feed based on how many changes are currently being processed.
============================================================================= */
function manageFlow () {
    if (changesProcessing >= parallelLimit && !feed.is_paused) {
        // pause processing til we drop down below 10
        feed.pause();
    } else if (changesProcessing < parallelLimit && feed.is_paused) {
        feed.resume();
    }
}


/*  Maybe Say
    A function to maybe console.log something. It depends on if the user wants it or not
============================================================================= */
function maybeSay (words) {
    if (beNoisy) console.log(words);
}


/*  Take Note
    A common way of noting any log type stuff. 
    Not that robust but it works and I abuse it sometimes for things other than errors.
    Even though this uses an async appendFile we aren't taking any callbacks because
    this is sparta.
============================================================================= */
function takeNote (doingWhat, package_name, error) {
    var errorHeader = "\nERROR: " + doingWhat + "   PACKAGE: " + package_name + "   TIME: " + JSON.stringify(new Date()) + "\n";
    maybeSay(errorHeader);
    fs.appendFile(logFile, errorHeader + JSON.stringify(error, null, 2), function (err) {
        if (err) {
            console.log("ERROR ERROR ERROR!!!    takeNote() couldn't write to the file");
        }
    });
}


/*  Stop Feed, available to end users. 
    loops every second, checking to see if changes are still being
    persisted to Postgres. If changes processing reaches 0, or 60+ attempts have
    been made, we call the callback.
============================================================================= */
exports.stopFeedAndProcessing = function (cb) {
    if (feed) {
        feed.stop();
    }
    var attempts = 0;
    function loopUntilFinished () {
        if (changesProcessing === 0 || attempts > 60) {
            cb();
        } else {
            attempts++;
            setTimeout(loopUntilFinished, 1000);
        }
    }
    loopUntilFinished();
};


/*     Copy The Data function
    This one starts the process, and is really the only thing available to end users
============================================================================= */
exports.copyTheData = function (config) {
    if (!config.couchUrl) {
        console.log('Please review the documentation as npm-postgre-mashup has changed.');
        process.exit();
    }
    couchUrl         = config.couchUrl;
    postgresHost     = config.postgresHost;
    postgresDatabase = config.postgresDatabase;
    postgresUser     = config.postgresUser;
    postgresPassword = config.postgresPassword;
    if (config.logFile) logFile = config.logFile;
    if (config.beNoisy) beNoisy = true;
    if (config.emptyPostgres) emptyPostgres = true;
    if (config.onCatchup) onCatchup = config.onCatchup;
    
    // clear out the log file for this run
    fs.writeFileSync(logFile, "", {encoding: 'utf8'});

    knex = knex.initialize({
        client: 'pg',
        connection: {
            host: postgresHost,
            database: postgresDatabase,
            user: postgresUser,
            password: postgresPassword
        }
    });
    
    initPostgres();                
};


/*  Gets the postgres database ready for the data.
    It first migrates postgres up to the latest schemaversion if it isn't there already
    Then it gets the latest sequence processed, in case we are resuming the feed
    from a previous run.
    If the end user decided to empty the database, postgres is migrated all the way down
    and back up, which will drop and recreate tables and things.
    Finally, the function to start following CouchDB is called.
============================================================================= */
function initPostgres () {
    postgrator.config.set({
        migrationDirectory: __dirname + "/migrations",
        driver: "pg",
        host: postgresHost,
        database: postgresDatabase,
        username: postgresUser,
        password: postgresPassword
    });
    postgrator.migrate(schemaversion, function (err) {
        if (err) {
            takeNote("Migrating up to " + schemaversion, "", err);
            throw(err); // if we can't migrate, there is no use in continuing.
        }
        figureOutWhereWeLeftOff();
    });
}

function figureOutWhereWeLeftOff () {
    // TODO: this sequence is not reliable as a resume point. 
    // what happens if 1, 2, and 3 run. 3 finishes. process stops. 
    // 1 and 2 never made it to DB. Should start at 1 not 3.
    knex('load_log').min('seq').where("processing", 1).exec(function (err, res) {
        if (err) maybeSay(err);
        if (res && res[0] && res[0].min) {
            startingSeq = res[0].min;
            maybeEmptyPostgres();
        } else {
            knex('load_log').max('seq').exec(function (err, res) {
                if (err) maybeSay(err);
                if (res && res[0] && res[0].max) {
                    startingSeq = res[0].max;
                }
                maybeEmptyPostgres();
            });
        }
    });
    
}

function maybeEmptyPostgres () {
    
    if (startingSeq === 1 || emptyPostgres) {
        if (startingSeq > 1) startingSeq = 1;
        maybeSay("Either sequence history is not available, or you opted to empty postgres");
        postgrator.migrate('000', function (err) {
            if (err) { 
                takeNote("Migrating down to 000", "", err);
                throw(err); // if we can't migrate there is no use going forward
            }
            postgrator.migrate(schemaversion, function (err) {
                if (err) {
                    takeNote("Migrating up to " + schemaversion, "", err);
                    throw(err);
                }
                followCouch();
            });
        });
    } else {
        followCouch();
    }
}

/*  Starts following the CouchDB
============================================================================= */
function followCouch () {
    maybeSay("starting on sequence " + startingSeq);
    var opts = {
        db: couchUrl,
        since: startingSeq, 
        include_docs: true,
        heartbeat: 60 * 1000, // ms in which couch must responds
    };
    
    feed = new follow.Feed(opts);
    
    feed.on('change', function (change) {
        changesProcessing++;
        manageFlow();
        onChangeReceived(change, function () {
            changesProcessing--;
            manageFlow();
        });
    });
    
    feed.on('error', function(err) {
        takeNote("follow feed error", "", err);
        console.error('Follow Feed Error: Since Follow always retries, this must be serious');
        throw err;
    });
    
    feed.on('confirm', function (db) {
        maybeSay("npm db confirmed:");
        maybeSay(db); 
        //db.doc_count;
        //db.doc_del_count;
        //db.disk_size;
        //db.data_size;
        //db.update_seq;
    });
    
    feed.on('catchup', function (seq_id) {
        maybeSay('all caught up. last sequence: ' + seq_id);
        if (onCatchup) onCatchup();
    });

    feed.follow();
}


/*  This will be called for every change from couchdb. 
    It takes the doc and transforms it for sql, the puts it to postgres
============================================================================= */
var insertCount = 0;
var metricsBeginTime;
var metricsEndTime;

// keep track of what packages are being worked on at a time
// this way we don't try editing the same package at the same time.
// we could inspect the error thrown and ignore... but I'd rather avoid errors
// if at all possible. 
// wrapping things in a transactions, while safe and allows rollbacks, 
// does not prevent errors entirely.
var packagesProcessing = {}; 

function onChangeReceived (change, cb) {
    changeCount++;
    if (!metricsBeginTime) metricsBeginTime = new Date();
    
    // Every n changes we should log something interesting to look at.
    // like change count and inserts per second, for fun
    if (changeCount % 100 === 0) {
        metricsEndTime = new Date();
        var seconds = (metricsEndTime - metricsBeginTime) / 1000;
        var insertsPerSecond = Math.round(insertCount / seconds);
        maybeSay("changeCount: " + changeCount + "    insertsPerSecond: " + insertsPerSecond);
        
        // and then reset the data we accumulate
        insertCount = 0;
        metricsBeginTime = null;
        metricsEndTime = null;
    }
    
    async.waterfall([
        function initData (next){
            var data = {
                doc: change.doc, 
                seq: change.seq,
                packageName: change.doc._id
            };
            next(null, data);
        },
        function throttleByPackageName (data, next) {
            // If we are not yet working on a change for this package continue
            // otherwise, wait a second and check again.
            function continueIfOpen () {
                if (!packagesProcessing.hasOwnProperty(data.packageName)) {
                    packagesProcessing[data.packageName] = data.packageName;
                    next(null, data);
                } else {
                    maybeSay("already processing a change for " + data.packageName + ". waiting a sec...");
                    setTimeout(continueIfOpen, 10000);
                }
            }
            continueIfOpen();
        },
        function startLoadLog (data, next) {
            data.inserts_start = new Date();
            data.inserts_finish = null;
            // NOTE: the load log stuff should happen outside the transaction.
            // we don't want this getting rolled back.
            var loadLog = {
                seq: data.seq,
                package_name: data.packageName,
                inserts_start: data.inserts_start,
                processing: 1
            };
            knex("load_log").select("seq").where("seq", data.seq).exec(function (err, res) {
                if (err) {
                    next(err, data);
                } else {
                    if (res && res.length) {
                        // do an update
                        knex("load_log").where("seq", data.seq).update(loadLog).exec(function (err, res) {
                            next(err, data);
                        });
                    } else {
                        // do an insert
                        knex("load_log").insert(loadLog).exec(function (err, res) {
                            next(err, data);
                        });
                    }
                }
            });
        },
        function assemblePackageLevelInfo (data, next){
            var doc = data.doc;
            data.packageData = {
                package_name:      doc._id,
                version_latest:    (doc["dist-tags"] ? doc["dist-tags"].latest : null),
                version_rc:        (doc["dist-tags"] ? doc["dist-tags"].rc : null),
                _rev:              doc._rev,
                readme:            doc.readme,
                readme_filename:   doc.readmeFilename,
                time_created:      (doc.time ? new Date(doc.time.created) : null),
                time_modified:     (doc.time ? new Date(doc.time.modified) : null)
            };
            next(null, data);
        },
        function getAllVersionsForPackageFromDb (data, next) {
            data.versionsInDb = {};
            knex("version")
                .select("version")
                .where("package_name", data.packageData.package_name)
                .exec(function (err, res) {
                    if (err) {
                        next(err);
                    } else {
                       if (res && res.length) {
                           for (var record = 0; record < res.length; record++) {
                               var version = res[record].version;
                               data.versionsInDb[version] = version;
                           }
                       }
                       next(null, data);
                    }
                });
        },
        function assembleVersionData (data, next) {
            var doc = data.doc;
            data.inserts = {};
            data.inserts.version = [];
            data.inserts.version_contributor = [];
            data.inserts.version_maintainer = [];
            data.inserts.version_dependency = [];
            data.inserts.version_dev_dependency = [];
            data.inserts.version_keyword = [];
            data.inserts.version_bin = [];
            data.inserts.version_script = [];
            
            for (var v in doc.versions) {
                // if version is not in database...
                if (!data.versionsInDb[v]) {
                    var dv = doc.versions[v];
                    var version = {
                        package_name:     doc._id,
                        version:          v,
                        description:      dv.description,
                        author_name:      (dv.author ? dv.author.name : null),
                        author_email:     (dv.author ? dv.author.email : null),
                        author_url:       (dv.author ? dv.author.url : null),
                        repository_type:  (dv.repository ? dv.repository.type : null),
                        repository_url:   (dv.repository ? dv.repository.url : null),
                        main:             dv.main,
                        license:          dv.license,
                        homepage:         dv.homepage,
                        bugs_url:         (dv.bugs ? dv.bugs.url : null),
                        bugs_homepage:    (dv.bugs ? dv.bugs.homepage : null),
                        bugs_email:       (dv.bugs ? dv.bugs.email : null),
                        engine_node:      (dv.engines ? dv.engines.node : null),
                        engine_npm:       (dv.engines ? dv.engines.npm : null),
                        dist_shasum:      (dv.dist ? dv.dist.shasum : null),
                        dist_tarball:     (dv.dist ? dv.dist.tarball : null),
                        _from:            dv._from,
                        _resolved:        dv._resolved,
                        _npm_version:     dv._npmVersion,
                        _npm_user_name:   (dv._npmUser ? dv._npmUser.name : null),
                        _npm_user_email:  (dv._npmUser ? dv._npmUser.email : null),
                        time_created:     (doc.time ? new Date(doc.time[v]) : null)
                    };
                    data.inserts.version.push(version);
                    
                    // Version Contributor
                    if (dv.contributors && dv.contributors.length && dv.contributors instanceof Array) {
                        for (var c = 0; c < dv.contributors.length; c++) {
                            var contributor = dv.contributors[c];
                            if (contributor && (contributor.name || contributor.email)) {
                                data.inserts.version_contributor.push({
                                    package_name: doc._id,
                                    version: v,
                                    name: contributor.name, 
                                    email: contributor.email
                                }); 
                            }
                        }
                    }
                    
                    // Version Maintainers
                    if (dv.maintainers && dv.maintainers.length && dv.maintainers instanceof Array) {
                        for (var m = 0; m < dv.maintainers.length; m++) {
                            var maintainer = dv.maintainers[m];
                            if (maintainer && (maintainer.name || maintainer.email)) {
                                data.inserts.version_maintainer.push({
                                    package_name: doc._id,
                                    version: v,
                                    name: maintainer.name, 
                                    email: maintainer.email
                                });
                            }
                        }
                    }
                    
                    // Version Dependencies
                    if (dv.dependencies) {
                        for (var d in dv.dependencies) {
                            data.inserts.version_dependency.push({
                                package_name: doc._id,
                                version: v,
                                dependency_name: d,
                                dependency_version: dv.dependencies[d]
                            });
                        }
                    }
                    
                    // Version Dev Dependencies
                    if (dv.devDependencies) {
                        for (var devdep in dv.devDependencies) {
                            data.inserts.version_dev_dependency.push({
                                package_name: doc._id,
                                version: v,
                                dev_dependency_name: devdep,
                                dev_dependency_version: dv.devDependencies[devdep]
                            });
                        }
                    }
                    
                    // Version Keywords
                    if (dv.keywords && dv.keywords.length && dv.keywords instanceof Array) {
                        for (var k = 0; k < dv.keywords.length; k++) {
                            var keyword = dv.keywords[k];
                            if (keyword) data.inserts.version_keyword.push({
                                package_name: doc._id,
                                version: v,
                                keyword: keyword
                            });
                        }
                    } else if (dv.keywords && dv.keywords.length && typeof dv.keywords === 'string') {
                        // This is a string of 1 keyword (maybe these were supposed to be split out automatically?)
                        data.inserts.version_keyword.push({
                            package_name: doc._id,
                            version: v,
                            keyword: dv.keywords
                        });
                    }
                    
                    // Version Bin
                    if (dv.bin) {
                        for (var b in dv.bin) {
                            data.inserts.version_bin.push({
                                package_name: doc._id,
                                version: v,
                                bin_command: b, 
                                bin_file: dv.bin[b]
                            });
                        }
                    }
                    
                    // Version Scripts
                    if (dv.scripts) {
                        for (var s in dv.scripts) {
                            data.inserts.version_script.push({
                                package_name: doc._id,
                                version: v,
                                script_name: s,
                                script_text: dv.scripts[s]
                            });
                        }
                    }
                    
                } // end if version not in database;
            } // end for loop iterating over versions in the doc
            
            next(null, data);
            
        }, // end function assembleVersionData
        function beginTransaction (data, next) {
            
            knex.transaction(function(t) {
                data.tran = t;
                next(null, data);
            }).then(function(resp) {
                //console.log('Transaction complete.');
            }).catch(function(err) {
                console.log("rolled back " + data.packageData.package_name);
                //console.error(err);
            });
        }, 
        function updateOrInsertPackage (data, next) {
            var tran = data.tran;
            var pkg = data.packageData;
            knex("package")
                .transacting(tran)
                .select("package_name")
                .where("package_name", pkg.package_name)
                .exec(function (err, res) {
                    if (err) {
                        next(err, data);
                    } else {
                       if (res && res.length) {
                           // package found. gonna update
                           knex("package").transacting(tran).where("package_name", pkg.package_name).update(pkg).exec(function (err) {
                               if (err) next(err, data);
                               else next(null, data);
                           });
                       } else {
                           // no package found, gonna insert
                           knex("package").transacting(tran).insert(pkg).exec(function(err) {
                               if (err) next(err, data);
                               else next(null, data);
                           });
                       } 
                    }
                });
        },
        function doVersionInserts (data, next) {
            var versionsData = [];
            for (var tablename in data.inserts) {
                versionsData.push({
                    tablename: tablename,
                    tran: data.tran,
                    rows: data.inserts[tablename]
                });
            }
            async.eachSeries(versionsData, versionToPg, function (err) {
                next(err, data);
            });
        }
    ], function theEndOfProcessingAChange (err, data) {
        data.inserts_finish = new Date();
        // Update the inserts_finish time, as well as change the processing bit to false.
        // This needs to happen regardless of error or not. 
        var loadLog = {
            seq: data.seq,
            package_name: data.packageName,
            version_latest: data.packageData.version_latest,
            inserts_start: data.inserts_start,
            inserts_finish: data.inserts_finish,
            processing: 0
        };
        knex("load_log").where("seq", data.seq).update(loadLog).exec(function (err, res) {
            if (err) {
                takeNote("Error Finalizing load_log", data.packageName, err);
                maybeSay("Error Finalizing load_log for package " + data.packageName);
            }
        });
        
        if (err) {
            takeNote("versions import (unsure where)", data.packageName, err);
            maybeSay("An insert failed somewhere - check log for details");
            maybeSay("rolling back");
            if (data.tran) data.tran.rollback();
            errorCount++;
            if (errorCount < errorLimit) {
                cb();
            } else {
                console.log("Too many errors! Stopping this thing.");
                feed.stop();
                process.exit();
            }
        } else {
            if (data.tran) data.tran.commit('');
            cb();
        }
        // record that we are no longer processing a change for this package
        if (packagesProcessing[data.packageName]) delete packagesProcessing[data.packageName];
    });
    
} // end  onChangeReceived


/*  the function used to do the version inserts by async.eachSeries
============================================================================= */
function versionToPg (tableInfo, callback) {
    if (tableInfo.rows && tableInfo.rows.length) {
        insertCount = insertCount + tableInfo.rows.length;
        knex(tableInfo.tablename)
            .transacting(tableInfo.tran)
            .insert(tableInfo.rows)
            .exec(callback);
    } else {
        // move on to the next one, nothing to insert here
        callback(); 
    }
}