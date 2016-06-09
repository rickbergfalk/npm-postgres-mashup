var fs = require('fs');
var follow = require('follow');
var async = require('async');
var pg = require('pg');
var downloadCounts = require('npm-download-counts');
var _ = require('lodash');
var massive = require('massive');

var conString = "";
var changeCount = 0;    // every time we begin to process a change, this will be incremented
var errorLimit = 20;    // If this many errors happen, we'll stop persisting to postgres and quit
var errorCount = 0;
var schemaversion = 'max';
var startingSeq = 1;
var parallelLimit = 8;    // number of changes we'll process at once
var couchUrl;
var postgresHost;
var postgresUser;
var postgresPassword;
var postgresDatabase;
var buildSchema;
var reportingTablesRequested;
var logFile = process.cwd() + "/npm2pg-log.txt";
var massiveInstance;

// clear out the log/fix file for this run
fs.writeFileSync(logFile, "", {encoding: 'utf8'});


/*  query 
    convenience function to get a connection from postgres pool and run a query
============================================================================= */
function query (query, params, cb) {
    pg.connect(conString, function(err, client, done) {
        if (err) {
            // error connecting to postgres? we won't get far. Just die here
            throw err;
        }
        client.query(query, params, function (err, result) {
            done(); // release connection back to pool
            cb(err, result);
        });
    });
}

/*  log 
    logs stuff to the console, but also details to a file for further inspection
============================================================================= */
function log (header, details) {
    console.log(header);
    if (details) {
        console.log("additional details written to npm2pg-log.txt");
        detailsText = ""
        if (typeof details === "string") {
            detailsText = details;
        } else if (details) {
            detailsText = JSON.stringify(details, null, 2);
        }
        fs.appendFile(logFile, header + "\n" + detailsText + "\n", function (err) {
            if (err) {
                console.error("ERROR! log() couldn't write to the file");
            }
        });
    }
}


/*  fixNulls
    there might be \u0000 characters in readmes or descriptions (or anywhere really)
    these are unicode NULL characters and postgres does not like these.
    they must be removed.
    this function recursively traverses the object and cleans up any nulls it finds. 
============================================================================= */
function fixNulls (obj) {
    _.forOwn(obj, function (value, key) {
        if (_.isString(value)) {
            obj[key] = value.replace(/\u0000/g, "");
        } else if (_.isObject(value)) {
            fixNulls(value);
        }
    });
}



/*  Copy The Data function
    This one starts the process, and is really the only thing available to end users
============================================================================= */
exports.copyTheData = function (config) {
    couchUrl                 = config.couchUrl;
    postgresHost             = config.postgresHost;
    postgresDatabase         = config.postgresDatabase;
    postgresUser             = config.postgresUser;
    postgresPassword         = config.postgresPassword;
    buildSchema              = (config.buildSchema);
    reportingTablesRequested = (config.reportingTables);
    conString = "tcp://" + postgresUser + ":" + postgresPassword + "@" + postgresHost + "/" + postgresDatabase;
    
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
    massiveInstance = massive.connectSync({
        connectionString : conString,
        scripts: __dirname + "/db"
    });
    if (buildSchema) {
        massiveInstance.createSchema(function (err, result) {
            if (err) {
                errDoc = {
                    err: err,
                    result: result
                };
                log("Error building schema", errDoc);
            } else {
                log("Schema built successfully");
            }
            log("Exiting npm2pg.");
            process.exit();
        });
    } else {
        figureOutWhereWeLeftOff();
    }
}

function figureOutWhereWeLeftOff () {
    query('SELECT MAX(seq) AS maxseq FROM couch_seq_log;', [], function (err, result) {
        if (err) log(err);
        if (result && result.rows && result.rows[0] && result.rows[0].maxseq) {
            startingSeq = result.rows[0].maxseq;
        }
        followCouch();
    });
}



/*  Starts following the CouchDB
============================================================================= */
function followCouch () {
    log("Starting on sequence " + startingSeq);
    log('Use ctrl-c at any time to stop the feed processing.');
    var opts = {
        db: couchUrl,
        since: startingSeq, 
        include_docs: true,
        heartbeat: 60 * 1000, // ms in which couch must responds
    };
    var changesProcessing = 0; // used to track number of changes that are currently processing
    var feed = new follow.Feed(opts);
    
    feed.on('change', function (change) {
        changesProcessing++;
        if (changesProcessing >= parallelLimit && !feed.is_paused) {
            feed.pause();
        }
        processChangeDoc(change, function () {
            changesProcessing--;
            if (changesProcessing < parallelLimit && feed.is_paused) {
                feed.resume();
            }
        });
    });
    
    feed.on('error', function(err) {
        log("follow feed error", err);
        console.error('Follow Feed Error: Since Follow always retries, this must be serious');
        throw err;
    });
    
    feed.on('confirm', function (db) {
        log("npm db confirmed... Starting feed processing.");
    });
    
    feed.on('catchup', function (seq_id) {
        feed.stop();
        log('Packages from CouchDB caught up. Last sequence: ' + seq_id);
        if (reportingTablesRequested) {
            log("Building reporting tables");
            massiveInstance.createReportingTables(function (err, result) {
                if (err) {
                    console.error(err);
                    errDoc = {
                        err: err,
                        result: result
                    };
                    log("Error building reporting tables", errDoc);
                } else {
                    log("Reporting tables refreshed successfully");
                }
                log("Exiting npm2pg.");
                process.exit();
            });
        } else {
            log("Exiting npm2pg");
            process.exit();
        }
    });

    feed.follow();
}


/*  processChangeDoc
    
    This will be called for every change from couchdb. 
    Basically sticks an npm package from couch into Postgres JSONB
============================================================================= */
var metricsBeginTime;
var metricsEndTime;

// keep track of what packages are being worked on at a time
// this way we don't try editing the same package at the same time.
// this object will contain {packagename: seq}
var packagesProcessing = {}; 

function processChangeDoc (change, cb) {

    async.waterfall([
        function initData (next){
            fixNulls(change.doc);
            var data = {
                doc: change.doc, 
                seq: change.seq,
                packageName: change.doc._id,
                deleted: (change.deleted ? 1 : 0),
                change: change
            };
            next(null, data);
        },
        function throttleByPackageName (data, next) {
            // If we are not yet working on a change for this package continue
            // otherwise, wait a second and check again.
            function continueIfOpen () {
                if (!packagesProcessing.hasOwnProperty(data.packageName)) {
                    packagesProcessing[data.packageName] = data.seq;
                    next(null, data);
                } else {
                    log("Already processing a change for " + data.packageName + ". waiting a sec...");
                    setTimeout(continueIfOpen, 10000);
                }
            }
            continueIfOpen();
        },
        function logProgress (data, next) {
            changeCount++;
            if (!metricsBeginTime) metricsBeginTime = new Date();
            
            // Every 1000 changes we should log something interesting to look at.
            // like change count and inserts per second, for fun
            if (changeCount % 1000 === 0) {
                metricsEndTime = new Date();
                var seconds = (metricsEndTime - metricsBeginTime) / 1000;
                var packagesPerSecond = Math.round(1000 / seconds);
                log("CouchDB Changes     Packages processed: " + changeCount + "     Packages/sec: " + packagesPerSecond);
                // and then reset the data we accumulate
                metricsBeginTime = null;
                metricsEndTime = null;
            }
            
            // Every so often we should log the lowest seq we are currently processing - 1
            // This was changed from logging every sequence and the state it was in 
            // That ended up being a lot of db traffic and overcomplicated things for little benefit
            if (changeCount % 500 === 0) {
                var minseq;
                for (var p in packagesProcessing) {
                    var seq = packagesProcessing[p];
                    if (!minseq) minseq = seq;
                    else if (seq < minseq) minseq = seq;
                }
                if (!minseq) console.log("No minseq?");
                if (minseq) {
                    minseq--;
                    // log the sequence. If this fails no big deal just log that it failed
                    query('INSERT INTO couch_seq_log (seq, process_date) VALUES ($1, $2);', [minseq, new Date()], function (err, result) {
                        if (err) log(err);
                    });
                }
            }
            // proceed to next step
            next(null, data);
        },
        function checkIfPackageExists (data, next) {
            query("SELECT package_name FROM package_doc WHERE package_name = $1", [data.packageName], function (err, result) {
                if (err) {
                    next(err, data);
                } else {
                    data.packageExists = (result.rows && result.rows.length);
                    next(null, data);
                }
            });
        },
        function updateOrInsertPackage (data, next) {
            var sql;
            if (data.packageExists) {
                sql = "UPDATE package_doc SET doc = $1, deleted = $2 WHERE package_name = $3";
            } else {
                sql = "INSERT INTO package_doc (doc, deleted, package_name) VALUES ($1, $2, $3)";
            }
            query(sql, [data.doc, data.deleted, data.packageName], function (err, results) {
                next(err, data);
            });
        }
    ], function theEndOfProcessingAChange (err, data) {
        // record that we are no longer processing a change for this package
        if (packagesProcessing[data.packageName]) delete packagesProcessing[data.packageName];
        if (err) {
            data.err = err;
            log("An insert/update failed - " + data.packageName, data);
            errorCount++;
        }
        if (errorCount < errorLimit) {
            cb();
        } else {
            console.log("Reached error limit (" + errorLimit + "). Stopping this thing.");
            process.exit();
        }
    });
} // end  processChangeDoc
