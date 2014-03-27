// READER BEWARE!
// I indent with tabs, use semicolons (most of the time), and wrote this on windows.

var fs = require('fs');
var cradle = require('cradle');
var knex = require('knex');
var async = require('async');
var postgrator = require('postgrator');


/* 	Variables for Later
============================================================================= */
var changeCount = 0;              // every time we begin to process a change, this will be incremented
var errorLimit = 20;              // If this many errors happen, we'll stop persisting to postgres and quit
var errorCount = 0;
var changes = [];                 // All the changes we get from the CouchDb will be stored here (mostly just Ids)
var c;                            // The CouchDB Connection
var db;                           // The CouchDB Client (cradle)
var couchHost;
var couchDatabase;
var postgresHost;
var postgresUser;
var postgresPassword;
var postgresDatabase;
var beNoisy = false;              // if set to true we'll console.log progress
var logFile = __dirname + "/error-log.txt";
var theFinalCallback;
var start = new Date();
var finish;

/* 	Maybe Say
	A function to maybe console.log something. It depends on if the user wants it or not
============================================================================= */
function maybeSay (words) {
	if (beNoisy) console.log(words);
}


/* 	Take Note
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
};


/* 	Copy The Data function
	This one starts the process, and is really the only thing available to end users
============================================================================= */
exports.copyTheData = function (config) {
	
	couchHost        = config.couchHost;
	couchDatabase    = config.couchDatabase;
	postgresHost     = config.postgresHost;
	postgresDatabase = config.postgresDatabase;
	postgresUser     = config.postgresUser;
	postgresPassword = config.postgresPassword;
	if (config.logFile) logFile = config.logFile;
	if (config.beNoisy) beNoisy = true;
	if (config.theFinalCallback) theFinalCallback = config.theFinalCallback;
	
	if (!config.iUnderstandThisDropsTables) throw new Error("You may not understand that this drops tables, so we aren't running this");
	
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
	
	cradle.setup({
		host: couchHost,
		cache: false,              // I'm not sure why,
		raw: true                  // but cache: false and raw: true make things work
	});

	c = new(cradle.Connection);
	db = c.database(couchDatabase);
	
	migratePostgresThenDoTheBigKickoff();				
};


/* 	Migrate the Postgres Database then do The Big Kickoff()
	We need to prep the Postgres Database so its ready for our data
	**Currently we're DROPPING ALL THE TABLES and recreating them.**
	This will need to change if we do a continuous feed import someday
============================================================================= */
function migratePostgresThenDoTheBigKickoff () {
	postgrator.config.set({
		migrationDirectory: __dirname + "/migrations",
		driver: "pg",
		host: postgresHost,
		database: postgresDatabase,
		username: postgresUser,
		password: postgresPassword
	});
	postgrator.migrate('000', function (err, migrations) {
		if (err) takeNote("Migrating down to 000", "", err);
		postgrator.migrate('001', function (err, migrations) {
			if (err) takeNote("Migrating up to 001", "", err);
			theBigKickoff();
		});
	});
};
	

/* 	The Big Kickoff
	This gets all the changes from CouchDB starting at the beginning.
	Once all the changes are gotten, we iterate over them
============================================================================= */
function theBigKickoff () {
	db.info(function(err, info) {
		if (err) throw err;
		maybeSay("committed update seq: " + info.committed_update_seq);
		var feed = db.changes({ since: 1 });
		feed.on('change', function (change) {
			changes.push(change);
			if (change.seq == info.committed_update_seq) {
				maybeSay('Got all the change seqs: ' + change.seq + ": " + change.id);
				// Now that we have all the changes, lets start iterating through the queue
				// for each change we'll get the document from CouchDB
				// turn it into a bunch of Postgres inserts and then run it against Postgres.
				async.eachLimit(changes, 5, takeOneDownPassItAround, function (err) {
					finish = new Date();
					var minutes = (finish - start) / 1000 / 60;
					maybeSay("all done. it took about " + Math.round(minutes) + " minutes.");
					if (theFinalCallback) { 
						theFinalCallback(err);
					} else {
						if (err) console.log("Errors happened so we ended early");
					}
				});
			}
		});
	});
};


/* 	Take One Down Pass It Around: a Persist to Postgres function.
	This will be called for every change from couchdb. It gets the doc
	from couch, then transforms it for sql, the puts it to postgres
============================================================================= */
function takeOneDownPassItAround (change, cb) {
	changeCount++;
	var changesLeft = changes.length - changeCount;
	if (changesLeft % 1000 === 0) {
		maybeSay("changes left: " + changesLeft);	
	}
	
	// the first step to handling a change is to get the document!
	db.get(change.id, function (err, doc) {

		if (err) {
			if (err.error === 'not_found' && err.reason === 'deleted') {
				// maybeSay("INFO: " + change.id + " not found because " + err.reason)
				cb()
			} else {
				takeNote('getting doc', change.id, err);
				errorCount++;
				if (errorCount < errorLimit) cb();
				else cb(err);
			}
		} else {

			var sqlInserts = {};

			/* first assemble the package level info
			-----------------------------------------------------------*/
			sqlInserts.package = {
				package_name: 		doc._id,
				version_latest: 	(doc["dist-tags"] ? doc["dist-tags"].latest : null),
				version_rc: 		(doc["dist-tags"] ? doc["dist-tags"].rc : null),
				_rev: 				doc._rev,
				readme: 			doc.readme,
				readme_filename: 	doc.readmeFilename,
				time_created: 		(doc.time ? new Date(doc.time.created) : null),
				time_modified: 		(doc.time ? new Date(doc.time.modified) : null)
			}

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
					package_name: 		doc._id,
					version: 			v,
					description: 		dv.description,
					author_name: 		(dv.author ? dv.author.name : null),
					author_email: 		(dv.author ? dv.author.email : null),
					author_url: 		(dv.author ? dv.author.url : null),
					repository_type: 	(dv.repository ? dv.repository.type : null),
					repository_url: 	(dv.repository ? dv.repository.url : null),
					main: 				dv.main,
					license: 			dv.license,
					homepage: 			dv.homepage,
					bugs_url: 			(dv.bugs ? dv.bugs.url : null),
					bugs_homepage: 		(dv.bugs ? dv.bugs.homepage : null),
					bugs_email: 		(dv.bugs ? dv.bugs.email : null),
					engine_node: 		(dv.engines ? dv.engines.node : null),
					engine_npm: 		(dv.engines ? dv.engines.npm : null),
					dist_shasum: 		(dv.dist ? dv.dist.shasum : null),
					dist_tarball: 		(dv.dist ? dv.dist.tarball : null),
					_from: 				dv._from,
					_resolved: 			dv._resolved,
					_npm_version: 		dv._npmVersion,
					_npm_user_name: 	(dv._npmUser ? dv._npmUser.name : null),
					_npm_user_email: 	(dv._npmUser ? dv._npmUser.email : null),
					time_created: 		(doc.time ? new Date(doc.time[v]) : null)
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
				};

				if (dv.maintainers && dv.maintainers.length && dv.maintainers instanceof Array) {
					dv.maintainers.forEach(function (m) {
						if (m && (m.name || m.email)) sqlInserts.versionMaintainers.push({
							package_name: doc._id,
							version: v,
							name: m.name, 
							email: m.email
						});	
					});
				};

				if (dv.dependencies) {
					for (var d in dv.dependencies) {
						sqlInserts.versionDependencies.push({
							package_name: doc._id,
							version: v,
							dependency_name: d,
							dependency_version: dv.dependencies[d]
						});
					};
				};

				if (dv.devDependencies) {
					for (var d in dv.devDependencies) {
						sqlInserts.versionDevDependencies.push({
							package_name: doc._id,
							version: v,
							dev_dependency_name: d,
							dev_dependency_version: dv.devDependencies[d]
						});
					};	
				};

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
				};

				if (dv.bin) {
					for (var b in dv.bin) {
						sqlInserts.versionBins.push({
							package_name: doc._id,
							version: v,
							bin_command: b, 
							bin_file: dv.bin[b]
						});
					};
				};

				if (dv.scripts) {
					for (var s in dv.scripts) {
						sqlInserts.versionScripts.push({
							package_name: doc._id,
							version: v,
							script_name: s,
							script_text: dv.scripts[s]
						});
					};
				};
			}

			/* 	Persist this stuff to Postgres
				
				This runs a bunch of functions designed for waterfall flow.
				I really like node.js, but sometimes stuff like this takes a lot of effort 
				to think through and write out in an elegant way. 
				(And I'm assuming/hoping this is considered elegant - that could be a stretch)
				
				Also, note that the first function is being added 
				to bootstrap the waterfall with the sqlInserts data.
				Is there not a way to start an async.waterfall with some data?
			-----------------------------------------------------------*/
			async.waterfall([
				function (next) {
					next(null, sqlInserts);
				},
				insertPackage,
				insertVersions,
				insertVersionContributors,
				insertVersionDependencies,
				insertVersionDevDependencies,
				insertVersionKeywords,
				insertVersionMaintainers,
				insertVersionBins,
				insertVersionScripts
			], function (err, result) {
				if (err) {
					console.log("An insert failed somewhere - check log for details");
					errorCount++;
					if (errorCount < errorLimit) cb();
					else cb(err);
				} else {
					cb();
				}
			});

		} // end have doc
	});
}


/* 	insert functions
	These will run in waterfall.
	Each of these functions insert into a table if data is in the array provided
	If not, the insert is skipped, 
	and the callback is called via a setImmediate to remain async'y
============================================================================= */
function insertPackage (inserts, next) {
	knex("package").insert(inserts.package).exec(function (err, res) { 
		if (err) {
			takeNote("package.insert()", inserts.package.package_name, err);
			next(err);
		} else {
			next(null, inserts);
		}
	});
};

function insertVersions (inserts, next) {
	if (inserts.versions.length) {
		knex("version").insert(inserts.versions).exec(function (err, res) {
			if (err) takeNote("version.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});
	} else {
		setImmediate(function () {
			next(null, inserts);
		});
	}
};

function insertVersionContributors (inserts, next) {
	if (inserts.versionContributors.length) {
		knex("version_contributor").insert(inserts.versionContributors).exec(function (err, res) {
			if (err) takeNote("version_contributor.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};

function insertVersionMaintainers (inserts, next) {
	if (inserts.versionMaintainers.length) {
		knex("version_maintainer").insert(inserts.versionMaintainers).exec(function (err, res) {
			if (err) takeNote("version_maintainer.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};

function insertVersionDependencies (inserts, next) {
	if (inserts.versionDependencies.length) {
		knex("version_dependency").insert(inserts.versionDependencies).exec(function (err, res) {
			if (err) takeNote("version_dependency.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};

function insertVersionDevDependencies (inserts, next) {
	if (inserts.versionDevDependencies.length) {
		knex("version_dev_dependency").insert(inserts.versionDevDependencies).exec(function (err, res) {
			if (err) takeNote("version_dev_dependency.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};

function insertVersionKeywords (inserts, next) {
	if (inserts.versionKeywords.length) {
		knex("version_keyword").insert(inserts.versionKeywords).exec(function (err, res) {
			if (err) takeNote("version_keyword.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};

function insertVersionBins (inserts, next) {
	if (inserts.versionBins.length) {
		knex("version_bin").insert(inserts.versionBins).exec(function (err, res) {
			if (err) takeNote("version_bin.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};

function insertVersionScripts (inserts, next) {
	if (inserts.versionScripts.length) {
		knex("version_script").insert(inserts.versionScripts).exec(function (err, res) {
			if (err) takeNote("version_script.insert()", inserts.package.package_name, err);
			next(err, inserts);
		});		
	} else {
		setImmediate(function () {
			next(null, inserts);
		});	
	}
};
