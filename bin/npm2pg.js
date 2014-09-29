#!/usr/bin/env node

var npmpm = require('../npm-postgres-mashup.js');
var program = require('commander');

program
  .version('1.0.0')
  .option('-r, --registry [url]',        'set registry url (default: skimdb.npmjs.com/registry)', 'https://skimdb.npmjs.com/registry')
  .option('-h, --host [host]',           'set postgres host (default: localhost)', 'localhost') // default to localhost if not provided
  .option('-d, --database [database]',   'set postgres database')
  .option('-u, --user [user]',           'set postgres user', '' )
  .option('-p, --password [password]',   'set postgres password')
  .option('-s, --stop-on-catchup',       'stop processing when caught up')
  .option('-e, --empty',                 'empty postgres database and start fresh')
  .parse(process.argv);


var catchup = function () {
    console.log("All caught up!");
    console.log("Processing will continue.");
    console.log('To stop at any time, type "stop"');
};

if (program.stopOnCatchup) {
    console.log("Process will stop when caught up");
    catchup = function () {
        console.log("All caught up!");
        console.log("Stopping the feed...");
        npmpm.stopFeedAndProcessing(function () {
            console.log('Stopped the feed');
            console.log('Exiting now');
            process.exit();
        });
    };
}

if (program.empty) console.log("Postgres database will be emptied");


npmpm.copyTheData({
    couchUrl: program.registry,
    postgresHost: program.host,
    postgresDatabase: program.database,
    postgresUser: program.user,
    postgresPassword: program.password,
    beNoisy: true,
    emptyPostgres: (program.empty), 
    onCatchup: catchup
});