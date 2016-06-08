#!/usr/bin/env node

/*

TARGET API

npm2pg -d npm2pg -u myuser -p mypassword --save

npm2pg --downloadcounts

npm2pg --empty

*/


var fs = require('fs');
var path = require('path');
var npmpm = require('../npm-postgres-mashup.js');
var program = require('commander');
var userHome = (process.platform === "win32" ? process.env.USERPROFILE : process.env.HOME );
var configFilePath = path.join(userHome, '.npm2pgrc');
console.log('');

var packageJson = require('../package.json');

// NOTE: default values are not handled via commander since we might look to a file for them
program
  .version(packageJson.version)
  .option('-r, --registry [url]',        'set registry url (default: skimdb.npmjs.com/registry)')
  .option('-h, --host [host]',           'set postgres host (default: localhost)')
  .option('-d, --database [database]',   'set postgres database')
  .option('-u, --user [user]',           'set postgres user', '' )
  .option('-p, --password [password]',   'set postgres password')
  .option('--build-schema',              'create or update schema *IMPORTANT* THIS WILL REMOVE TABLES')
  .option('--reporting-tables',          'builds reporting tables after catching up with registry')
  .option('--save',                      'saves some parameters to $HOME/.npm2pgrc to use next run')
  .option('--forget',                    'forgets any previously saved parameters. exits immediately after')
  .parse(process.argv);

if (program.save) {
    console.log('Saving config to ' + configFilePath);
    var conf = {
        registry: program.registry,
        host: program.host,
        database: program.database,
        user: program.user,
        password: program.password,
        reportingTables: program.reportingTables
    };
    fs.writeFileSync(configFilePath, JSON.stringify(conf, null, 2));
}

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
    console.log('Loading saved config.');
    console.log('Parameters passed in via cli will override saved values.')
    console.log('To remove saved config run npm2pg --forget');
    
    var conf = JSON.parse(fs.readFileSync(configFilePath));
    console.log('\nsaved config:');
    console.log(JSON.stringify(conf, null, 2));
    
    program.registry = program.registry || conf.registry;
    program.host = program.host ||  conf.host;
    program.database = program.database || conf.database;
    program.user = program.user || conf.user;
    program.password = program.password || conf.password;
    program.reportingTables = program.reportingTables || conf.reportingTables;
    console.log("");
}

// pass in any defaults here instead of using commander. 
// this allows us to provide defaults via .npm2pgrc file
program.registry = program.registry || 'https://skimdb.npmjs.com/registry';
program.host = program.host || 'localhost';


npmpm.copyTheData({
    couchUrl: program.registry,
    postgresHost: program.host,
    postgresDatabase: program.database,
    postgresUser: program.user,
    postgresPassword: program.password,
    buildSchema: program.buildSchema,
    reportingTables: program.reportingTables
});