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
var stopOnCatchup = false;
var onCatchup;

var feed; // follow feed
var startingSeq = 1;
var parallelLimit = 1;
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
    if (config.stopOnCatchup) stopOnCatchup = true;
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
        if (stopOnCatchup) {
            feed.stop();
            process.exit();
        }
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

    var sqlInserts = {};

    /* first assemble the package level info
    -----------------------------------------------------------*/
    sqlInserts.package = {
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
    -----------------------------------------------------------*/
    sqlInserts.versions = [];
    sqlInserts.versionContributors = [];
    sqlInserts.versionMaintainers = [];
    sqlInserts.versionDependencies = [];
    sqlInserts.versionDevDependencies = [];
    sqlInserts.versionKeywords = [];
    sqlInserts.versionBins = [];
    sqlInserts.versionScripts = [];

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
        sqlInserts.versions.push(version);

        // For each version, also do the other things...
        if (dv.contributors && dv.contributors.length && dv.contributors instanceof Array) {
            dv.contributors.forEach(function (c) {
                if (c && (c.name || c.email)) sqlInserts.versionContributors.push({
                    package_name: doc._id,
                    version: v,
                    name: c.name, 
                    email: c.email
                });    
            });
        }

        if (dv.maintainers && dv.maintainers.length && dv.maintainers instanceof Array) {
            dv.maintainers.forEach(function (m) {
                if (m && (m.name || m.email)) sqlInserts.versionMaintainers.push({
                    package_name: doc._id,
                    version: v,
                    name: m.name, 
                    email: m.email
                });    
            });
        }

        if (dv.dependencies) {
            for (var d in dv.dependencies) {
                sqlInserts.versionDependencies.push({
                    package_name: doc._id,
                    version: v,
                    dependency_name: d,
                    dependency_version: dv.dependencies[d]
                });
            }
        }

        if (dv.devDependencies) {
            for (var devdep in dv.devDependencies) {
                sqlInserts.versionDevDependencies.push({
                    package_name: doc._id,
                    version: v,
                    dev_dependency_name: devdep,
                    dev_dependency_version: dv.devDependencies[devdep]
                });
            }
        }

        if (dv.keywords && dv.keywords.length && dv.keywords instanceof Array) {
            dv.keywords.forEach(function (k) {
                if (k) sqlInserts.versionKeywords.push({
                    package_name: doc._id,
                    version: v,
                    keyword: k
                });
            });
        } else if (dv.keywords && dv.keywords.length && typeof dv.keywords === 'string') {
            // This is a string of 1 keyword (maybe these were supposed to be split out automatically?)
            sqlInserts.versionKeywords.push({
                package_name: doc._id,
                version: v,
                keyword: dv.keywords
            });
        }

        if (dv.bin) {
            for (var b in dv.bin) {
                sqlInserts.versionBins.push({
                    package_name: doc._id,
                    version: v,
                    bin_command: b, 
                    bin_file: dv.bin[b]
                });
            }
        }

        if (dv.scripts) {
            for (var s in dv.scripts) {
                sqlInserts.versionScripts.push({
                    package_name: doc._id,
                    version: v,
                    script_name: s,
                    script_text: dv.scripts[s]
                });
            }
        }
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
    
    knex.transaction(function(t) {
        sqlInserts.transaction = t;
        var delete_start;
        var delete_finish;
        var inserts_start;
        var inserts_finish;
        async.waterfall([
            function (next) {
                delete_start = new Date();
                knex("package").transacting(sqlInserts.transaction).where('package_name', doc._id).del().exec(function (err) {
                    delete_finish = new Date();
                    if (err) maybeSay(err);
                    inserts_start = new Date();
                    next(null, sqlInserts);
                });
            },
            function insertPackage (inserts, next) {
                knex("package").transacting(inserts.transaction).insert(inserts.package).exec(function (err) { 
                    if (err) takeNote("package.insert()", inserts.package.package_name, err);
                    next(err, inserts);
                });
            },
            function insertVersions (inserts, next) {
                insertIfNecessary(inserts, next, "version", "versions");
            },
            function insertVersionContributors (inserts, next) {
                insertIfNecessary(inserts, next, "version_contributor", "versionContributors");
            },
            function insertVersionMaintainers (inserts, next) {
                insertIfNecessary(inserts, next, "version_maintainer", "versionMaintainers");
            },
            function insertVersionDependencies (inserts, next) {
                insertIfNecessary(inserts, next, "version_dependency", "versionDependencies");
            },
            function insertVersionDevDependencies (inserts, next) {
                insertIfNecessary(inserts, next, "version_dev_dependency", "versionDevDependencies");
            },
            function insertVersionKeywords (inserts, next) {
                insertIfNecessary(inserts, next, "version_keyword", "versionKeywords");
            },
            function insertVersionBins (inserts, next) {
                insertIfNecessary(inserts, next, "version_bin", "versionBins");
            },
            function insertVersionScripts (inserts, next) {
                insertIfNecessary(inserts, next, "version_script", "versionScripts");
            }
        ], function (err) {
            inserts_finish = new Date();
            var load_log = {
                seq: change.seq,
                package_name: sqlInserts.package.package_name,
                version_latest: sqlInserts.package.version_latest,
                delete_start: delete_start,
                delete_finish: delete_finish,
                inserts_start: inserts_start,
                inserts_finish: inserts_finish
            };
            
            // log to postgres
            // it doesn't need to prevent the rest of the process from continuing
            knex("load_log").where("seq", load_log.seq).del().exec(function (err) {
                if (err) maybeSay("error deleting load_log.seq. shouldn't need to do this...");
                knex("load_log").insert(load_log).exec(function (err) {
                    if (err) {
                        maybeSay("Couldn't log load activity in load_log. See error log for details.");
                        takeNote("load_log.insert()", sqlInserts.package.package_name, err);
                    }
                });
            });
            
            // handle error, and continue on regardless
            if (err) {
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
            
        });
    }).then(function () {
        // it was committed? 
        // Normally we'd continue here, but I'm not using the promises api so that's happening somewhere else
        // promise-flow is messing with my callback flow :(
    }, function (err) {
        maybeSay('rolled back: ' + (err || '')); 
    });
}


/*  insert functions
    These will run in waterfall.
    Each of these functions insert into a table if data is in the array provided
    If not, the insert is skipped, 
    and the callback is called via a setImmediate to remain async'y
============================================================================= */
function insertIfNecessary (inserts, next, table, insertsProperty) {
    if (inserts[insertsProperty].length) {
        knex(table).transacting(inserts.transaction).insert(inserts[insertsProperty]).exec(function (err) {
            if (err) takeNote(table + ".insert()", inserts.package.package_name, err);
            next(err, inserts);
        });
    } else {
        setImmediate(function () {
            next(null, inserts);
        });
    }
}
