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

var feed; // follow feed
var startingSeq = 1;
var parallelLimit = 10;
var emptyPostgres = false;

/*     Maybe Say
    A function to maybe console.log something. It depends on if the user wants it or not
============================================================================= */
function maybeSay (words) {
    if (beNoisy) console.log(words);
}


/*     Take Note
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


exports.stopFeed = function () {
    if (feed) feed.stop();
}


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
    if (config.onCatchup) onCatchup = config.onCatchup;
    if (config.emptyPostgres) emptyPostgres = true;
    
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


/*     
    Migrate the Postgres Database then do The Big Kickoff()
    We need to prep the Postgres Database so its ready for our data
    **Currently we're DROPPING ALL THE TABLES and recreating them.**
    This will need to change if we do a continuous feed import someday
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
    postgrator.migrate('002', function (err) {
        if (err) {
            takeNote("Migrating up to 002", "", err);
            throw(err); // if we can't migrate, there is no use in continuing.
        }
        knex('load_log').max('seq').exec(function (err, res) {
            if (err) maybeSay(err);
            if (res && res[0] && res[0].max && !emptyPostgres) {
                startingSeq = res[0].max;
                followCouch();
            } else {
                maybeSay("Either max seq is not present, or user opted to empty postgres");
                postgrator.migrate('000', function (err) {
                    if (err) { 
                        takeNote("Migrating down to 000", "", err);
                        throw(err); // if we can't migrate there is no use going forward
                    }
                    postgrator.migrate('002', function (err) {
                        if (err) {
                            takeNote("Migrating up to 002", "", err);
                            throw(err);
                        }
                        followCouch();
                    });
                });
            }
        });
    });
}

var changesProcessing = 0;    
function manageFlow () {
    if (changesProcessing >= parallelLimit && !feed.is_paused) {
        // pause processing til we drop down below 10
        feed.pause();
    } else if (changesProcessing < parallelLimit && feed.is_paused) {
        feed.resume();
    }
}

/*  The Big Kickoff
    This gets all the changes from CouchDB starting at the beginning.
    Once all the changes are gotten, we iterate over them
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
        persistToPg(change, function () {
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


/*  Persist to Postgres function.
    This will be called for every change from couchdb. 
    It takes the doc and transforms it for sql, the puts it to postgres
============================================================================= */
function persistToPg (change, cb) {
    changeCount++;
    var doc = change.doc;
    if (changeCount % 10 === 0) {
        maybeSay("changeCount: " + changeCount);   
    }

    var packageData = {};
    var versionsData = [];
    
    /* first assemble the package level info
    -----------------------------------------------------------*/
    packageData = {
        package_name:         doc._id,
        version_latest:     (doc["dist-tags"] ? doc["dist-tags"].latest : null),
        version_rc:         (doc["dist-tags"] ? doc["dist-tags"].rc : null),
        _rev:                 doc._rev,
        readme:             doc.readme,
        readme_filename:     doc.readmeFilename,
        time_created:         (doc.time ? new Date(doc.time.created) : null),
        time_modified:         (doc.time ? new Date(doc.time.modified) : null)
    };

    /* then for each version, assemble all the version level info
    
    {
        package: {
            package_name: "", 
            version_latest: ""
            etc
        },
        versions: [
            {
                tablename: "version_contributor",
                package_name: "my-module",
                version: "0.1.2",
                rows: [{}, {}, {}]
            }
        ]
        
    }
    -----------------------------------------------------------*/

    for (var v in doc.versions) {
        var dv = doc.versions[v];
        var version = {
            package_name:         doc._id,
            version:             v,
            description:         dv.description,
            author_name:         (dv.author ? dv.author.name : null),
            author_email:         (dv.author ? dv.author.email : null),
            author_url:         (dv.author ? dv.author.url : null),
            repository_type:     (dv.repository ? dv.repository.type : null),
            repository_url:     (dv.repository ? dv.repository.url : null),
            main:                 dv.main,
            license:             dv.license,
            homepage:             dv.homepage,
            bugs_url:             (dv.bugs ? dv.bugs.url : null),
            bugs_homepage:         (dv.bugs ? dv.bugs.homepage : null),
            bugs_email:         (dv.bugs ? dv.bugs.email : null),
            engine_node:         (dv.engines ? dv.engines.node : null),
            engine_npm:         (dv.engines ? dv.engines.npm : null),
            dist_shasum:         (dv.dist ? dv.dist.shasum : null),
            dist_tarball:         (dv.dist ? dv.dist.tarball : null),
            _from:                 dv._from,
            _resolved:             dv._resolved,
            _npm_version:         dv._npmVersion,
            _npm_user_name:     (dv._npmUser ? dv._npmUser.name : null),
            _npm_user_email:     (dv._npmUser ? dv._npmUser.email : null),
            time_created:         (doc.time ? new Date(doc.time[v]) : null)
        };
        versionsData.push({
            tablename: "version",
            package_name: doc._id,
            version: v,
            rows: version
        });

        // For each version, also do the other things...
        var versionContributors = [];
        if (dv.contributors && dv.contributors.length && dv.contributors instanceof Array) {
            for (var c = 0; c < dv.contributors.length; c++) {
                var contributor = dv.contributors[c];
                if (contributor && (contributor.name || contributor.email)) {
                    versionContributors.push({
                        package_name: doc._id,
                        version: v,
                        name: contributor.name, 
                        email: contributor.email
                    }); 
                }
            }
        }
        if (versionContributors.length) versionsData.push({
            tablename: "version_contributor",
            package_name: doc._id,
            version: v,
            rows: versionContributors
        });
        
        // Version Maintainers
        var versionMaintainers = [];
        if (dv.maintainers && dv.maintainers.length && dv.maintainers instanceof Array) {
            for (var m = 0; m < dv.maintainers.length; m++) {
                var maintainer = dv.maintainers[m];
                if (maintainer && (maintainer.name || maintainer.email)) versionMaintainers.push({
                    package_name: doc._id,
                    version: v,
                    name: maintainer.name, 
                    email: maintainer.email
                });
            }
        }
        if (versionMaintainers.length) versionsData.push({
            tablename: "version_maintainer",
            package_name: doc._id,
            version: v,
            rows: versionMaintainers
        });
        
        
        // Version Dependencies
        var versionDependencies = [];
        if (dv.dependencies) {
            for (var d in dv.dependencies) {
                versionDependencies.push({
                    package_name: doc._id,
                    version: v,
                    dependency_name: d,
                    dependency_version: dv.dependencies[d]
                });
            }
        }
        if (versionDependencies.length) versionsData.push({
            tablename: "version_dependency",
            package_name: doc._id,
            version: v,
            rows: versionDependencies
        });
        
        // Version Dev Dependencies
        var versionDevDependencies = [];
        if (dv.devDependencies) {
            for (var devdep in dv.devDependencies) {
                versionDevDependencies.push({
                    package_name: doc._id,
                    version: v,
                    dev_dependency_name: devdep,
                    dev_dependency_version: dv.devDependencies[devdep]
                });
            }
        }
        if (versionDevDependencies.length) versionsData.push({
            tablename: "version_dev_dependency",
            package_name: doc._id,
            version: v,
            rows: versionDevDependencies
        });
        
        // Version Keywords
        var versionKeywords = [];
        if (dv.keywords && dv.keywords.length && dv.keywords instanceof Array) {
            for (var k = 0; k < dv.keywords.length; k++) {
                var keyword = dv.keywords[k];
                if (keyword) versionKeywords.push({
                    package_name: doc._id,
                    version: v,
                    keyword: keyword
                });
            }
        } else if (dv.keywords && dv.keywords.length && typeof dv.keywords === 'string') {
            // This is a string of 1 keyword (maybe these were supposed to be split out automatically?)
            versionKeywords.push({
                package_name: doc._id,
                version: v,
                keyword: dv.keywords
            });
        }
        if (versionKeywords.length) versionsData.push({
            tablename: "version_keyword",
            package_name: doc._id,
            version: v,
            rows: versionKeywords
        });
        
        // Version Bin
        var versionBins = [];
        if (dv.bin) {
            for (var b in dv.bin) {
                versionBins.push({
                    package_name: doc._id,
                    version: v,
                    bin_command: b, 
                    bin_file: dv.bin[b]
                });
            }
        }
        if (versionBins.length) versionsData.push({
            tablename: "version_bin",
            package_name: doc._id,
            version: v,
            rows: versionBins
        });
        
        
        // Version Scripts
        var versionScripts = [];
        if (dv.scripts) {
            for (var s in dv.scripts) {
                versionScripts.push({
                    package_name: doc._id,
                    version: v,
                    script_name: s,
                    script_text: dv.scripts[s]
                });
            }
        }
        if (versionScripts.length) versionsData.push({
            tablename: "version_script",
            package_name: doc._id,
            version: v,
            rows: versionScripts
        });
    }

    /*  Persist this stuff to Postgres
        
        This runs a bunch of functions designed for waterfall flow.
        I really like node.js, but sometimes stuff like this takes a lot of effort 
        to think through and write out in an elegant way. 
        (And I'm assuming/hoping this is considered elegant - that could be a stretch)
        
        Also, note that the first function is being added 
        to bootstrap the waterfall with the sqlInserts data.
        Is there not a way to start an async.waterfall with some data?
        
        
        
    -----------------------------------------------------------*/
    
    function packageToPg (tran, pkg, callback) {
        knex("package")
            .transacting(tran)
            .select("package_name")
            .where("package_name", pkg.package_name)
            .exec(function (err, res) {
                if (err) {
                    callback(err);
                } else {
                   if (res && res.length) {
                       // package found. gonna update
                       knex("package").transacting(tran).where("package_name", pkg.package_name).update(pkg).exec(callback);
                   } else {
                       // no package found, gonna insert
                       knex("package").transacting(tran).insert(pkg).exec(callback);
                   } 
                }
            });
    }
    
    function versionToPg (tableInfo, callback) {
        // check if data already exists for package_name and version.
        // if so, move on
        // otherwise, insert data
        knex(tableInfo.tablename)
            .transacting(tableInfo.tran)
            .select("package_name")
            .where({"package_name": tableInfo.package_name, "version": tableInfo.version})
            .exec(function (err, res) {
                if (err) {
                    callback(err);
                } else {
                    if (res && res.length) {
                        // data found for package version. skip doing anything else
                        callback();
                    } else {
                        // no data found for that package version. insert
                        knex(tableInfo.tablename)
                            .transacting(tableInfo.tran)
                            .insert(tableInfo.rows)
                            .exec(callback);
                    }
                }
            });
    }
    
    var inserts_start = new Date();
    var inserts_finish;
    knex.transaction(function(t) {
        
        // bootstrap transaction onto each version table set
        versionsData.forEach(function(version) {
            version.tran = t;
        });
        
        packageToPg(t, packageData, function (err) {
            if (err) {
                takeNote("package import", packageData.package_name, err);
                
            } else {
                async.eachSeries(versionsData, versionToPg, function (err) {
                    inserts_finish = new Date();
                    
                    // log to postgres
                    // it doesn't need to prevent the rest of the process from continuing
                    var load_log = {
                        seq: change.seq,
                        package_name: packageData.package_name,
                        version_latest: packageData.version_latest,
                        inserts_start: inserts_start,
                        inserts_finish: inserts_finish
                    };
                    knex("load_log").insert(load_log).exec(function (err) {
                        if (err) {
                            maybeSay("Couldn't log load activity in load_log. See error log for details.");
                            takeNote("load_log.insert()", packageData.package_name, err);
                        }
                    });
                    
                    if (err) {
                        takeNote("versions import (unsure where)", packageData.package_name, err);
                        maybeSay("An insert failed somewhere - check log for details");
                        maybeSay("rolling back");
                        t.rollback();
                        errorCount++;
                        if (errorCount < errorLimit) {
                            cb();
                        } else {
                            console.log("Too many errors! Stopping this thing.");
                            process.exit();
                        }
                    } else {
                        t.commit('');
                        cb();
                    }
                })
            }
        });
       
    }).then(function () {
        // it was committed? 
        // Normally we'd continue here, but I'm not using the promises api so that's happening somewhere else
        // promise-flow is messing with my callback flow :(
    }, function (err) {
        maybeSay('rolled back: ' + (err || '')); 
    });
}
