# npm Postgres Mashup

npm-postgres-mashup is a little utility to do a one-time data replication from npm's skimdb to a postgres database. It handles most of the npm data, although there are some cases this library doesn't deal with. 

This is a library for fun, and does not aim to recreate npm in Postgres. This is more of an experiment to answer those nagging questions everyone seems to have like, "What would npm look like if it were stored in a relational database?" or "How would it feel to work with npm if I had to deal with a relational model?"

Curious about the database design, or the results of running this script without running it yourself? You can read about it in [readme-too.md](readme-too.md)?


## Usage

First, replicate the npm skimdb to a local CouchDB. While this would probably work pointed directly against npm's database, it's probably impolite to do so, especially if you find yourself running it multiple times. This isn't the most efficient npm replication, so have a local CouchDB available to abuse.

Got the local copy? Then do some programming:

```js
var yetAnotherNpm = require('npm-postgres-mashup.js');

yetAnotherNpm.copyTheData({
    couchHost: 'hopefully localhost',
    couchDatabase: 'whatever you called your npm',
    postgresHost: 'probably localhost',
    postgresDatabase: 'datbase',
    postgresUser: 'user',
    postgresPassword: 'password',
    beNoisy: true,                        // if you want to be console.logged a lot
    iUnderstandThisDropsTables: false,    // you  must agree to TOS variable 
    theFinalCallback: function (err) {    // called when everything is done 
        if (err) throw err;               // or ends early because of too many errors
        console.log('all done');
    }
});
```

To successfully use this module you need to set ```iUnderstandThisDropsTables``` to ```true```. I'm lazy and this module is lazy, so when you run this after the initial run, each time it drops the npm tables and recreates them. 


## Installation

```
npm install npm-postgres-mashup
```


## License

MIT
