// READER BEWARE!
// I indent with tabs, use semicolons (most of the time), and wrote this on windows.

var fs = require('fs');
var follow = require('follow');
var async = require('async');
var postgrator = require('postgrator');
var getVersionParts = require('./lib/get-version-parts.js');
var select = require('sql-bricks').select;
var insert = require('sql-bricks').insert;
var update = require('sql-bricks').update;
var pg = require('pg.js');
var downloadCounts = require('npm-download-counts');
var conString = "";


/*     Variables for Later
============================================================================= */
var changeCount = 0;    // every time we begin to process a change, this will be incremented
var errorLimit = 20;    // If this many errors happen, we'll stop persisting to postgres and quit
var errorCount = 0;
var couchUrl;
var postgresHost;
var postgresUser;
var postgresPassword;
var postgresDatabase;
var beNoisy = false;              // if set to true we'll console.log progress
var logFile = process.cwd() + "/npm2pg-error-log.txt";
var fixFile = process.cwd() + "/npm2pg-fix-log.txt";
var onCatchup;

var schemaversion = '016';

var feed; // follow feed
var startingSeq = 1;
var emptyPostgres = false;

var parallelLimit = 8;    // number of changes we'll process at once
var changesProcessing = 0; // used to track number of changes that are currently processing
var downloadsProcessing = 0; 
var doDownloads = true;
var couchCaughtUp = false;
var downloadsCaughtUp = false;

/*  Manage Flow
    stops/starts feed based on how many changes are currently being processed.
============================================================================= */
function manageFlow () {
    if (changesProcessing >= parallelLimit && !feed.is_paused) {
        // pause processing til we drop down below 10
        //maybeSay("pausing. Changes processing: " + changesProcessing);
        feed.pause();
    } else if (changesProcessing < parallelLimit && feed.is_paused) {
        //maybeSay("resuming. Changes processing: " + changesProcessing);
        feed.resume();
    }
}


/*  Maybe Say
    A function to maybe console.log something. It depends on if the user wants it or not
============================================================================= */
function maybeSay (words) {
    if (beNoisy) console.log(words);
}

function ensureString (thing, field) {
    if (typeof thing === "string") {
        return thing;
    } else if (typeof thing === "undefined") {
        return null;
    } else if (thing === null) {
        return null;
    } else {
        var text = "\n\n" + field + "\n" + JSON.stringify(thing, null, 2);
        return JSON.stringify(thing, null, 2);
    }
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
            console.error("ERROR ERROR ERROR!!!    takeNote() couldn't write to the file");
        }
    });
}


/*  Stop Feed, available to end users. 
    loops every second, checking to see if changes are still being
    persisted to Postgres. If changes processing reaches 0, or 60+ attempts have
    been made, we call the callback.
============================================================================= */
exports.stopFeedAndProcessing = function (cb) {
    doDownloads = false;
    if (feed) {
        feed.stop();
    }
    var attempts = 0;
    function loopUntilFinished () {
        if ((changesProcessing === 0 && downloadsProcessing === 0) || attempts > 60) {
            cb();
        } else {
            attempts++;
            setTimeout(loopUntilFinished, 1000);
        }
    }
    loopUntilFinished();
};


/*  Allow the end user to stop the process safely 
============================================================================= */
process.stdin.resume();
process.stdin.setEncoding('utf8');
process.stdin.on('data', function (data) {
    data = (data + '').trim().toLowerCase();
    if (data === 'stop') {
        console.log('stopping. please hold.');
        exports.stopFeedAndProcessing(function () {
            console.log('Changes have stopped processing.');
            process.exit();
        });
    }
});


/*     Copy The Data function
    This one starts the process, and is really the only thing available to end users
============================================================================= */
exports.copyTheData = function (config) {
    if (!config.couchUrl) {
        console.log('Please review the documentation as npm-postgres-mashup has changed.');
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
    
    // clear out the log/fix file for this run
    fs.writeFileSync(logFile, "", {encoding: 'utf8'});
    fs.writeFileSync(fixFile, "", {encoding: 'utf8'});
    
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
    pg.connect(conString, function(err, client, done) {
        if (err) {
            // error connecting to postgres? we won't get far. Just die here
            throw err;
        }
        client.query('SELECT MIN(seq) AS minseq FROM load_log WHERE processing = CAST(1 AS BIT)', function (err, result) {
            if (err) maybeSay(err);
            if (result && result.rows && result.rows[0] && result.rows[0].minseq) {
                startingSeq = result.rows[0].minseq - 1;
                done(); // release connection back to pool
                maybeEmptyPostgres();
            } else {
                client.query('SELECT MAX(seq) AS maxseq FROM load_log', function (err, result) {
                    if (err) maybeSay(err);
                    if (result.rows[0] && result.rows[0].maxseq) {
                        startingSeq = result.rows[0].maxseq;
                    }
                    done();
                    maybeEmptyPostgres();
                });
            }
        });
    });
}

function truncateTables (cb) {
    var truncateSql = "TRUNCATE TABLE version_script; \n"
    + "TRUNCATE TABLE version_maintainer; \n"
    + "TRUNCATE TABLE version_keyword; \n"
    + "TRUNCATE TABLE version_dev_dependency; \n"
    + "TRUNCATE TABLE version_dependency; \n"
    + "TRUNCATE TABLE version_contributor; \n"
    + "TRUNCATE TABLE version_bin; \n"
    + "TRUNCATE TABLE version; \n"
    + "TRUNCATE TABLE download_count; \n"
    + "TRUNCATE TABLE package; \n";
    pg.connect(conString, function(err, client, done) {
        if (err) {
            console.error("error connecting before truncate:");
            console.error(err);
            done();
            cb();
        } else {
            client.query(truncateSql, function (err, result) {
                if (err) {
                    console.error("error running truncate sql:");
                    console.error(err);
                }
                done();
                cb();
            });
        }
    });
}

function maybeEmptyPostgres () {
    if (startingSeq === 1 || emptyPostgres) {
        if (startingSeq > 1) startingSeq = 1;
        maybeSay("Either sequence history is not available, or you opted to empty postgres");
        truncateTables(function () {
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
                    gatherDownloadCounts();
                });
            });
        });
    } else {
        followCouch();
        gatherDownloadCounts();
    }
}

/*  Starts the download count gatherer
    This gatherer isn't the most efficient, but thats okay
    because this approach hopefully won't bombard the download counts service
    it is self throttling in a way. hopefully.
============================================================================= */
function gatherDownloadCounts () {
    function processNextBatch () {
        if (doDownloads) {
            var startTime = new Date();
            getDownloadCountBatch(function (err, result) {
                if (result.rows && result.rows.length) {
                    async.eachLimit(result.rows, 10, getDownloadsForPackage, function (err) {
                        if (err) console.error(err);
                        var endTime = new Date();
                        var seconds = (endTime - startTime) / 1000;
                        maybeSay("Download counts for 1000 packages in " + seconds + " seconds.");
                        processNextBatch();
                    });
                } else {
                    // no packages to process
                    maybeSay("All caught up with download counts");
                    if (downloadsProcessing === 0) downloadsCaughtUp = true;
                    if (onCatchup && couchCaughtUp && downloadsProcessing === 0) onCatchup();
                }
            });
        }
    }
    maybeSay("Starting Download Counts in 10 seconds...");
    setTimeout(processNextBatch, 10000);
}

function getDownloadCountBatch (cb) {
    var getPackageSql = "SELECT p.package_name, COALESCE(p.last_download_count_day + INTERVAL '1 Day', p.time_created) AS start_date, DATE 'yesterday' AS end_date "
        + "FROM package p "
        + "WHERE p.last_download_count_day IS NULL OR p.last_download_count_day < DATE 'yesterday' "
        + "LIMIT 1000";
    
    pg.connect(conString, function(err, client, done) {
        if (err) {
            console.error("error connecting for process_next_package:");
            console.error(err);
            done();
            cb(err);
        } else {
            client.query(getPackageSql, function (err, result) {
                done();
                cb(err, result);
            });
        }
    });
}

function getDownloadsForPackage (packageInfo, cb) {
    var package_name = packageInfo.package_name;
    var start_date = packageInfo.start_date;
    var end_date = packageInfo.end_date;
    downloadCounts(package_name, start_date, end_date, function (err, data) {
        if (err || !data) {
            // most likely there was no data for this range
            // despite this error, we just want to skip it
            pg.connect(conString, function (err, client, done) {
                client.query("UPDATE package SET last_download_count_day = DATE 'yesterday' WHERE package_name = $1;", [package_name], function (err, result) {
                    if (err) {
                        console.error("error updating last_download_count_day for " + package_name);
                        console.error(err);
                    }
                    done();
                    cb();
                });
            });
        } else {
            var insertSet = [];
            data.forEach(function (row) {
                insertSet.push({
                    package_name: package_name,
                    download_date: row.day,
                    download_count: row.count
                });
            });
            var sql;
            var sqlBricksErr;
            try {
                sql = insert("download_count", insertSet).toString();    
            } 
            catch (e) {
                sqlBricksErr = e;
                var sqlBricksErrText = "\n\n"
                    + "tableInfo.tablename:\n"
                    + "download_count" + "\n"
                    + "tableInfo.rows:\n"
                    + JSON.stringify(insertSet, null, 2)
                    + "\n\n"
                    + JSON.stringify(e);
                takeNote("Error generating sql", "download_count", e);
                fs.appendFile(fixFile, sqlBricksErrText, function (err) {
                    if (err) console.error("ERROR ERROR ERROR! Couldn't write to FIX FILE!!! :(");
                });
            }
            if (sqlBricksErr) {
                console.error("Download count error, retrying in 10 sec.");
                console.error(sqlBricksErr);
                cb();
            } else {
                downloadsProcessing = downloadsProcessing + 1;
                pg.connect(conString, function (err, client, done) {
                    client.query(sql, function (err, result) {
                        if (err) {
                            console.error("error running download count insert sql:");
                            console.error(err);
                            done();
                            cb();
                        } else {
                            client.query("UPDATE package SET last_download_count_day = DATE 'yesterday' WHERE package_name = $1;", [package_name], function (err, result) {
                                if (err) {
                                    console.error("error updating last_download_count_day for " + package_name);
                                    console.error(err);
                                }
                                downloadsProcessing = downloadsProcessing - 1;
                                done();
                                cb();
                            });
                        }
                    });
                });
            }
        }
    });
}




/*  Starts following the CouchDB
============================================================================= */
function followCouch () {
    maybeSay("Starting on sequence " + startingSeq);
    maybeSay('Type "stop" at any time to safetly stop the feed processing.');
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
        maybeSay("npm db confirmed...");
        maybeSay("Starting feed processing.");
    });
    
    feed.on('catchup', function (seq_id) {
        maybeSay('All caught up. Last sequence: ' + seq_id);
        couchCaughtUp = true;
        if (onCatchup && downloadsCaughtUp) onCatchup();
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
    if (changeCount % 1000 === 0) {
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
                    maybeSay("Already processing a change for " + data.packageName + ". waiting a sec...");
                    setTimeout(continueIfOpen, 10000);
                }
            }
            continueIfOpen();
        },
        function getClientFromPool (data, next) {
            pg.connect(conString, function(err, client, done) {
                if (err) {
                    // error connecting to postgres? we won't get far. Just die here
                    throw err;
                } 
                // assign the client and done function to the data object. 
                // its going to get passed around and used here and there.
                data.client = client;
                data.done = done;
                next(null, data);
            });    
        },
        function startLoadLog (data, next) {
            var client = data.client;
            data.inserts_start = new Date();
            data.inserts_finish = null;
            // NOTE: the load log stuff should happen outside the transaction.
            // we don't want this getting rolled back.
            var loadLog = {
                seq: data.seq,
                package_name: data.packageName,
                inserts_start: data.inserts_start,
                processing: '1'
            };
            client.query("SELECT seq FROM load_log WHERE seq = $1", [data.seq], function (err, result) {
                if (err) {
                    next(err, data);
                } else {
                    var sql;
                    if (result.rows.length) {
                        // do an update
                        sql = update('load_log', loadLog).where({"seq": data.seq}).toString();
                    } else {
                        //do an insert
                        sql = insert('load_log', loadLog).toString();
                    }
                    client.query(sql, function(err, result) {
                        next(err, data);
                    });
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
                readme:            ensureString(doc.readme, "readme"),
                readme_filename:   doc.readmeFilename,
                time_created:      (doc.time ? new Date(doc.time.created) : null),
                time_modified:     (doc.time ? new Date(doc.time.modified) : null)
            };
            next(null, data);
        },
        function getAllVersionsForPackageFromDb (data, next) {
            var client = data.client;
            data.versionsInDb = {};
            client.query("SELECT version FROM version WHERE package_name = $1", [data.packageData.package_name], function (err, result) {
                if (err) {
                    next(err, data);
                } else {
                    if (result.rows.length) {
                        for (var record = 0; record < result.rows.length; record++) {
                            var version = result.rows[record].version;
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
                    var versionParts = getVersionParts(v);
                    
                    // clean up some of the problematic data
                    var cleanLicense = ""
                    if (typeof dv.license === "string") {
                        cleanLicense = dv.license;
                    } else if (dv.license instanceof Array) {
                        // if this is an array of strings, simply join the list of
                        // licenses together. not optimal or good db design, 
                        // but its something
                        if (typeof dv.license[0] === "string") {
                            cleanLicense = dv.license.join(", ");
                        }
                        // if this license is an array of objects, 
                        // attempt to build a license via name or type of that object
                        else if (dv.license[0] && (dv.license[0].type || dv.license[0].name)) {
                            var licenses = [];
                            for (var i = 0; i < dv.license.length; i++) {
                                licenses.push(dv.license[i].type || dv.license[i].name || dv.license[i].url || "");
                            }
                            cleanLicense = licenses.join(", ");
                        }
                    } else if (dv.license && (dv.license.type || dv.license.name || dv.license.url)) {
                        cleanLicense = dv.license.type || dv.license.name || dv.license.url;
                    }
                    
                    // clean up homepage
                    var cleanHomepage = "";
                    if (typeof dv.homepage === "string") {
                        cleanHomepage = dv.homepage;
                    } else if (dv.homepage && dv.homepage.url) {
                        cleanHomepage = dv.homepage.url;  
                    } else if (dv.homepage instanceof Array && typeof dv.homepage[0] === "string") {
                        cleanHomepage = dv.homepage.join(", ");
                    } else if (dv.homepage instanceof Array && dv.homepage[0] && dv.homepage[0].url) {
                        var urls = [];
                        for (var i = 0; i < dv.homepage.length; i++) {
                            urls.push(dv.homepage[i].url);
                        }
                        cleanHomepage = urls.join(", ");
                    } else {
                        cleanHomepage = dv.homepage;
                    }
                    
                    // clean up description
                    var cleanDescription = dv.description;
                    if (typeof dv.description === "string") {
                        cleanDescription = dv.description;
                    } else if (dv.description instanceof Array) {
                        cleanDescription = dv.description.join(" ");
                    }
                    
                    var version = {
                        package_name:        doc._id,
                        version:             v,
                        description:         ensureString(cleanDescription, "description"),
                        author_name:         (dv.author ? dv.author.name : null),
                        author_email:        (dv.author ? dv.author.email : null),
                        author_url:          (dv.author ? dv.author.url : null),
                        repository_type:     (dv.repository ? ensureString(dv.repository.type, "repository.type") : null),
                        repository_url:      (dv.repository ? ensureString(dv.repository.url, "repository.url") : null),
                        main:                ensureString(dv.main, "dv.main"),
                        license:             ensureString(cleanLicense, "license"),
                        homepage:            ensureString(cleanHomepage, "homepage"),
                        bugs_url:            (dv.bugs ? dv.bugs.url : null),
                        bugs_homepage:       (dv.bugs ? dv.bugs.homepage : null),
                        bugs_email:          (dv.bugs ? dv.bugs.email : null),
                        engine_node:         (dv.engines ? ensureString(dv.engines.node, "engines.node") : null),
                        engine_npm:          (dv.engines ? ensureString(dv.engines.npm, "engines.npm") : null),
                        dist_shasum:         (dv.dist ? ensureString(dv.dist.shasum, "dist.shasum") : null),
                        dist_tarball:        (dv.dist ? ensureString(dv.dist.tarball, "dist.tarball") : null),
                        _from:               ensureString(dv._from, "_from"),
                        _resolved:           ensureString(dv._resolved, "_resolved"),
                        _npm_version:        ensureString(dv._npmVersion, "_npm_version"),
                        _npm_user_name:      (dv._npmUser ? dv._npmUser.name : null),
                        _npm_user_email:     (dv._npmUser ? dv._npmUser.email : null),
                        time_created:        (doc.time ? new Date(doc.time[v]) : null),
                        version_patch:       versionParts.patch,
                        version_major:       versionParts.major,
                        version_minor:       versionParts.minor,
                        version_label:       versionParts.label,
                        version_is_stable:   (versionParts.isStable ? '1' : '0') // was ints not string
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
                                dependency_name: ensureString(d, "dependency_name"),
                                dependency_version: ensureString(dv.dependencies[d], "dependency_version")
                            });
                        }
                    }
                    
                    // Version Dev Dependencies
                    if (dv.devDependencies) {
                        for (var devdep in dv.devDependencies) {
                            data.inserts.version_dev_dependency.push({
                                package_name: doc._id,
                                version: v,
                                dev_dependency_name: ensureString(devdep, "dev_dependency_name"),
                                dev_dependency_version: ensureString(dv.devDependencies[devdep], "dev_dependency_version")
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
                                script_text: ensureString(dv.scripts[s], "script_text")
                            });
                        }
                    }
                    
                } // end if version not in database;
            } // end for loop iterating over versions in the doc
            
            next(null, data);
            
        }, // end function assembleVersionData
        function beginTransaction (data, next) {
            var client = data.client;
            client.query("BEGIN", function (err, result) {
                if (!err) {
                    data.tran = true;
                }
                next(err, data);
            });
        }, 
        function updateOrInsertPackage (data, next) {
            var client = data.client;
            var pkg = data.packageData;
            
            client.query("SELECT package_name FROM package WHERE package_name = $1", [pkg.package_name], function (err, result) {
                if (err) {
                    next(err, data);
                } else {
                    var sql;
                    var sqlBuildError;
                    if (result.rows.length) {
                        // package found do update
                        try {
                            sql = update("package", pkg).where({"package_name": pkg.package_name}).toString();
                        }
                        catch (e) {
                            sqlBuildError = e;
                            console.log(pkg);
                            console.log(e);
                        }
                    } else {
                        // no package found, do insert
                        try {
                            sql = insert("package", pkg).toString();
                        }
                        catch (e) {
                            sqlBuildError = e;
                            console.log(pkg);
                            console.log(e);
                        }
                    }
                    if (sqlBuildError) {
                        next(sqlBuildError, data);
                    } else {
                        client.query(sql, function (err, result) {
                            next(err, data);
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
                    rows: data.inserts[tablename],
                    client: data.client
                });
            }
            async.eachSeries(versionsData, versionToPg, function (err) {
                next(err, data);
            });
        },
        function doPreviousStableUpdate (data, next) {
            var client = data.client;
            client.query("SELECT update_previous_versions($1)", [data.packageData.package_name], function (err, result) {
                if (err) {
                    next(err, data);
                } else {
                    next(null, data);
                }
            });
        }
    ], function theEndOfProcessingAChange (err, data) {
        var client = data.client;
        var done = data.done;
        data.inserts_finish = new Date();
        
        if (err) {
            takeNote("versions import (unsure where)", data.packageName, err);
            maybeSay("An insert failed somewhere - check log for details");
            errorCount++;
            if (data.tran) {
                maybeSay("rolling back");
                client.query("ROLLBACK", function (err, result) {
                    lastLoadLog();
                });
            } else {
                // there wasn't a transaction. we didn't make it far.
                // wrap this up and do another (maybe);
                lastLoadLog();
            }    
        } else {
            client.query("COMMIT", function (err, result) {
                lastLoadLog();
            });
        }
        
        function lastLoadLog () {
            // Update the inserts_finish time, as well as change the processing bit to false.
            // This needs to happen regardless of error or not. 
            var loadLog = {
                seq: data.seq,
                package_name: data.packageName,
                version_latest: data.packageData.version_latest,
                inserts_start: data.inserts_start,
                inserts_finish: data.inserts_finish,
                processing: '0'
            };
            var sql = update("load_log", loadLog).where({"seq": data.seq}).toString();
            client.query(sql, function (loadLogError, result) {
                if (loadLogError) {
                    takeNote("Error Finalizing load_log", data.packageName, err);
                    maybeSay("Error Finalizing load_log for package " + data.packageName);
                }
                wrapThisChangeUp();
            });
        }
        
        function wrapThisChangeUp () {
            done();
            // record that we are no longer processing a change for this package
            if (packagesProcessing[data.packageName]) delete packagesProcessing[data.packageName];
            
            if (errorCount < errorLimit) {
                cb();
            } else {
                console.log("Too many errors! Stopping this thing.");
                feed.stop();
                process.exit();
            }
        };
        
    });
    
} // end  onChangeReceived


/*  the function used to do the version inserts by async.eachSeries
============================================================================= */
function versionToPg (tableInfo, callback) {
    if (tableInfo.rows && tableInfo.rows.length) {
        insertCount = insertCount + tableInfo.rows.length;
        var client = tableInfo.client;
        var sql;
        var sqlBricksErr;
        var sqlBricksErrText;
        try {
            sql = insert(tableInfo.tablename, tableInfo.rows).toString();    
        } 
        catch (e) {
            sqlBricksErr = e;
            sqlBricksErrText = "\n\n"
                + "tableInfo.tablename:\n"
                + tableInfo.tablename + "\n"
                + "tableInfo.rows:\n"
                + JSON.stringify(tableInfo.rows, null, 2)
                + "\n\n"
                + JSON.stringify(e)
            takeNote("Error generating sql", tableInfo.tablename, e);
            fs.appendFile(fixFile, sqlBricksErrText, function (err) {
                if (err) console.log("ERROR ERROR ERROR! Couldn't write to FIX FILE!!! :(");
            });
        }
        if (sqlBricksErr) {
            // if there was a sqlBricksErr, skip this round of inserts
            callback(sqlBricksErr);
        } else {
            client.query(sql, callback);
        }
    } else {
        // move on to the next one, nothing to insert here
        callback(); 
    }
}