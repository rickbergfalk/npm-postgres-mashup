#!/usr/bin/env node

var fs = require('fs');
var path = require('path');
var npmpm = require('../npm-postgres-mashup.js');
var program = require('commander');
var userHome = (process.platform === "win32" ? process.env.USERPROFILE : process.env.HOME );
var configFilePath = path.join(userHome, '.npm2pgrc');
console.log('');

program
  .version('1.0.0')
  .option('-r, --registry [url]',        'set registry url (default: skimdb.npmjs.com/registry)', 'https://skimdb.npmjs.com/registry')
  .option('-h, --host [host]',           'set postgres host (default: localhost)', 'localhost') // default to localhost if not provided
  .option('-d, --database [database]',   'set postgres database')
  .option('-u, --user [user]',           'set postgres user', '' )
  .option('-p, --password [password]',   'set postgres password')
  .option('-s, --stop-on-catchup',       'stop processing when caught up')
  .option('-e, --empty',                 'empty postgres database and start fresh')
  .option('--save',                      'saves passed in parameters to $HOME/.npm2pgrc to use next time')
  .option('--forget',                    'forgets any previously saved parameters. exits immediately after')
  .parse(process.argv);


if (program.forget) {
    // delete the config file
    if (fs.existsSync(configFilePath)) {
        fs.unlinkSync(configFilePath);
        console.log("Previous configuration removed.");
    } else {
        console.log("No previous configuration saved. Maybe it was a different user?");
    }
    console.log("Now exiting...");
    process.exit();
}


// try to open existing config if it exists
if (fs.existsSync(configFilePath)) {
    console.log('loading saved config. This will override whatever was passed in');
    console.log('to remove saved config run npm2pg --forget');
    
    var conf = JSON.parse(fs.readFileSync(configFilePath));
    console.log('\nsaved config:');
    console.log(JSON.stringify(conf, null, 2));
    
    program.registry = conf.registry || program.registry;
    program.host = conf.host ||  program.host;
    program.database = conf.database || program.database;
    program.user = conf.user || program.user;
    program.password = conf.password || program.password;
    program.stopOnCatchup = conf.stopOnCatchup || program.stopOnCatchup;
    program.empty = conf.empty || program.empty;
}


if (program.save) {
    console.log('saving config to ' + configFilePath);
    var conf = {
        registry: program.registry,
        host: program.host,
        database: program.database,
        user: program.user,
        password: program.password,
        stopOnCatchup: program.stopOnCatchup,
        empty: program.empty
    };
    fs.writeFileSync(configFilePath, JSON.stringify(conf, null, 2));
}


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