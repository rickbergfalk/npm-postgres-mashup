// READER BEWARE!
// I indent with tabs, use semicolons and wrote this on windows.

var fs = require('fs');
var cradle = require('cradle');
var knex = require('knex');
var async = require('async');

var logFile = __dirname + "/error-log.txt";
fs.writeFileSync(logFile, "", {endocing: "utf8"});

knex = knex.initialize({
	client: 'pg',
	connection: {
		host: 'localhost', //'blue2.corecloud.com',
		user: 'npmadmin',
		password: 'testadmin',
		database: 'npm'
	}
});

cradle.setup({
	host: 'localhost', //"http://blue2.corecloud.com",
	cache: false,
	raw: true
});

var c = new(cradle.Connection);
var db = c.database('mynpm');
var changes = [];

// every time we begin to process a change, this will be incremented
var changeCount = 0; 

// if we hit an error, we just want to ignore it and move on to the next module. 
// Unless we've had this many. Then something is probably really wrong or should be addressed.
var errorLimit = 2000; 
var errorCount = 0;


var packageIdCounter = 0;
var versionIdCounter = 0;
var keywordIdCounter = 0;
var index = {
	package: {
		// "postgrator": 1
	},
	version: {
		// "postgrator": {
		//     '0.1.1': 1	
		// }
	},
	keyword: {
		// "test": 1
	}
};

///
/// INT ID VERSION
/// 
db.info(function(err, info) {
	if (err) throw err;
	console.log(info.committed_update_seq);
	var feed = db.changes({ since: 1, include_docs: true });
	feed.on('change', function (change) {
		changes.push({id: change.id});
		
		// index package name, versions, and keywords
		packageIdCounter++;
		index.package[change.id] = packageIdCounter;
		
		for (var v in change.doc.versions) {
			versionIdCounter++;
			var dv = change.doc.versions[v];
			if (!index.version[change.id]) index.version[change.id] = {};
			index.version[change.id][v] = versionIdCounter;
			
			if (dv.keywords && dv.keywords.length && dv.keywords instanceof Array) {
				dv.keywords.forEach(function (k) {
					if (!index.keyword[k.trim()]) {
						keywordIdCounter++;
						index.keyword[k.trim()] = keywordIdCounter;
						
					}
				});
			}
		}
		
		
		if (change.seq == info.committed_update_seq) {
			console.log('this is the final change: ' + change.seq + ": " + change.id);
			console.log(JSON.stringify(change, null, 2));
			// Now that we have all the changes, lets start iterating through the queue
			// for each change we'll get the document from CouchDB
			// turn it into a bunch of Postgres inserts and then run it against Postgres.
			
			// Insert all the keywords
			// Empty the table first though
			var keywordInserts = [];
			for (var k in index.keyword) {
				keywordInserts.push({keyword_id: index.keyword[k], keyword: k});	
			}
			knex("keyword").insert(keywordInserts).exec(function (err, res) {
				if (err) {
					takeNote("keyword.insert()", "all-the-packages", err);
				} else {
					async.eachSeries(changes, takeOneDownPassItAround, function (err) {
						if (err) console.log("Errors happened so we ended early");
						else console.log("all done");
					});	
				}
			});
			
			
				
		}
	});
});


function takeNote (doingWhat, package_name, error) {
	var errorHeader = "\nERROR: " + doingWhat + "   PACKAGE: " + package_name + "   TIME: " + JSON.stringify(new Date()) + "\n";
	console.log(errorHeader);
	fs.appendFile(logFile, errorHeader + JSON.stringify(error, null, 2), function (err) {
		if (err) {
			console.log("ERROR ERROR ERROR!!!    takeNote() couldn't write to the file");
		}
	});
};


/* 	Persist to Postgres, a function.
	This will be called every time we get a change from 
	couchdb. There won't be any follow up to its async options because 
	this is sparta.
============================================================================= */

function takeOneDownPassItAround (change, cb) {
	changeCount++;
	
	if (changeCount % 100 === 0) {
		console.log("changes left: " + (changes.length - changeCount));	
	}
	
	// the first step to handling a change is to get the document!
	db.get(change.id, function (err, doc) {

		if (err) {
			if (err.error === 'not_found' && err.reason === 'deleted') {
				// console.log("INFO: " + change.id + " not found because " + err.reason)
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
			var latest_version = (doc["dist-tags"] ? doc["dist-tags"].latest : null);
			var rc_version = (doc["dist-tags"] ? doc["dist-tags"].rc : null);
			
			// wondering what this is all about
			if (!index.version[doc._id]) {
				takeNote("They say this index doesn't exist", doc._id, doc);	
			}
			
			sqlInserts.package = {
				package_id: 		index.package[doc._id],
				package_name: 		doc._id,
				latest_version_id: 	(latest_version && index.version[doc._id] ? index.version[doc._id][latest_version] : null),
				rc_version_id: 		(rc_version && index.version[doc._id] ? index.version[doc._id][rc_version] : null),
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
					package_id: 		index.package[doc._id],
					version_id: 		index.version[doc._id][v],
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
					time_created: 		(dv.time ? new Date(doc.time[v]) : null)
				};
				sqlInserts.versions.push(version);

				// For each version, also do the other things...
				if (dv.contributors && dv.contributors.length && dv.contributors instanceof Array) {
					dv.contributors.forEach(function (c) {
						if (c && (c.name || c.email)) sqlInserts.versionContributors.push({
							version_id: index.version[doc._id][v],
							name: c.name, 
							email: c.email
						});	
					});
				} else if (dv.contributors && dv.contributors.length) {
					// if these aren't an array what are they?
					//takeNote("What's up with these contributors?", doc._id, dv.contributors);
				};

				if (dv.maintainers && dv.maintainers.length && dv.maintainers instanceof Array) {
					dv.maintainers.forEach(function (m) {
						if (m && (m.name || m.email)) sqlInserts.versionMaintainers.push({
							version_id: index.version[doc._id][v],
							name: m.name, 
							email: m.email
						});	
					});
				} else if (dv.maintainers && dv.maintainers.length) {
					// if these aren't an array what are they?
					//takeNote("What's up with these maintainers?", doc._id, dv.maintainers);
				};

				if (dv.dependencies) {
					for (var d in dv.dependencies) {
						sqlInserts.versionDependencies.push({
							version_id: index.version[doc._id][v],
							dependency_name: d,
							dependency_version: dv.dependencies[d]
						});
					};
				};

				if (dv.devDependencies) {
					for (var d in dv.devDependencies) {
						sqlInserts.versionDevDependencies.push({
							version_id: index.version[doc._id][v],
							dev_dependency_name: d,
							dev_dependency_version: dv.devDependencies[d]
						});
					};	
				};

				if (dv.keywords && dv.keywords.length && dv.keywords instanceof Array) {
					dv.keywords.forEach(function (k) {
						if (k) {
							var version_id = Number(index.version[doc._id][v]);
							var keyword_id = Number(index.keyword[k.trim()])
							if (isNaN(keyword_id)) {
								// the keyword_id is not a number?
								takeNote("keyword_id not a number?", doc._id, keyword_id);
							} else {
								sqlInserts.versionKeywords.push({
									version_id: version_id,
									keyword_id: keyword_id
								});	
							}
						}
					});
				} else if (dv.keywords && dv.keywords.length) {
					// if these aren't an array what are they?
					//takeNote("What's up with these keywords?", doc._id, dv.keywords);
				};

				if (dv.bin) {
					for (var b in dv.bin) {
						sqlInserts.versionBins.push({
							version_id: index.version[doc._id][v],
							bin_command: b, 
							bin_file: dv.bin[b]
						});
					};
				};

				if (dv.scripts) {
					for (var s in dv.scripts) {
						sqlInserts.versionScripts.push({
							version_id: index.version[doc._id][v],
							script_name: s,
							script_text: dv.scripts[s]
						});
					};
				};
			}

			/* Persist this stuff to Postgres
				- delete a pre-existing package (it'll cascade and remove everything)
				- re-add all the stuff

				This should be wrapped up in a transaction.
				Even better, this should be an update/add for new versions
				Right now though, this is just an experiment, so we're just gonna kill 'n fill.

				This runs a bunch of functions designed for waterfall flow.
				I really like node.js, but sometimes stuff like this takes a lot of effort 
				to think through and write out in an elegant way. 
				(And I'm assuming/hoping this is considered elegant - that could be a stretch)
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
	each of these functions insert into a table if data is in the array provided
	if not, the insert is skipped,
	and the callback is called
============================================================================= */
function insertPackage (inserts, next) {
	knex("package").where('package_name', inserts.package.package_name).del().exec(function (err, res) {
		if (err) takeNote("package.del()", inserts.package.package_name, err);
		knex("package").insert(inserts.package).exec(function (err, res) { 
			if (err) {
				takeNote("package.insert()", inserts.package.package_name, err);
				next(err);
			} else {
				next(null, inserts);
			}
		});
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
