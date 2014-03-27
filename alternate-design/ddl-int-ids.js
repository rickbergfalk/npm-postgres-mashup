var fs = require('fs');
var pg = require('pg.js');
var env = require('node-env-file');

env(__dirname + '/.env', {verbose: false, overwrite: true});

var conString = process.env.DATABASE_URL;

var ddlFile = fs.readFileSync(__dirname + "/ddl-int-ids.sql", {encoding: 'utf8'});

var client = new pg.Client(conString);
client.connect(function(err) {
  if(err) {
    return console.error('could not connect to postgres', err);
  }
  client.query(ddlFile, function(err, result) {
    if(err) {
      return console.error('error running query', err);
    }
    console.log(result);
    //output: Tue Jan 15 2013 19:12:47 GMT-600 (CST)
    client.end();
  });
});