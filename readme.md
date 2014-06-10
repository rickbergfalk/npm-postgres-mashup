# npm Postgres Mashup

For when you want to replicate most<sup>[1](#1)</sup> of the npm skimdb registry to a *relational* model inside Postgres. **Now with continuous replication and more efficiency!**



## Usage

Assuming you have a postgres database available:

```js  
var npmpm = require('npm-postgres-mashup');

npmpm.copyTheData({
    couchUrl: "https://skimdb.npmjs.com/registry",
    postgresHost: 'postgres-server',
    postgresDatabase: 'postgres-db-name',
    postgresUser: 'postgres-db-user',
    postgresPassword: 'postgres-db-password',
    
    // Optional. Defaults to false
    // Use if you want to be console.logged a lot and updated about progress
    beNoisy: true,             
    
    // Optional. Defaults to false
    // Use if you want to remove any npm postgres tables and start fresh
    // Otherwise processing will pick up where you left off
    emptyPostgres: true, 
    
    // fires once caught up
    onCatchup: function () {
        console.log("all caught up!");
        console.log("turn me off now and start again later...");
        console.log("or just let me keep going.");
        
        // use this to safetly stop processing the feed 
        npmpm.stopFeedAndProcessing(function () {
            console.log('stopped the feed');
            console.log('exiting now');
            process.exit(0);
        });
    }
});

```



## Installation

```
npm install npm-postgres-mashup
```



## Why?

This is a library for fun, and does not aim to recreate npm in Postgres. This is more of an experiment to answer those nagging question "What would npm look like if it were stored in a relational database?" or "How would it feel to work with npm if I had to deal with a relational model?"



## Fun Queries

See [readme-fun-queries](readme-fun-queries.md).



## Findings & Design Considerations

See [readme-too](readme-too.md).



## License

MIT



---------------------------
<a name="1"></a>
## What isn't Replicated


#### Packages with invalid UTF8 

Most of the registy is there, but not all. A handful of packages have invalid UTF8 encoding or something like that. They fail to insert and this doesn't try to address that.

#### Some of the obscure package/version properties

This module doesn't load all the properties for a given package/version. Most of the data is there though. If something is missing and you really want it, pull requests are welcome! 

#### An update to a package version (this should never happen)

In npm, a version can only ever be published once, so we assume there is no need to save a package version a second time the next time a package change comes in.

Due to the way couchdb works and the way npm (the company) is skimming the skimdb, once npm-postgres-mashup is caught up there will actually be 2 change documents per package version publish. The first change document presumably contains the binaries and fat that doesn't belong in the database. The skimdb daemon works to remove that fat and put it elsewhere, then updates the package with a fat-free version. This update causes the second change document, sometimes almost immediately after the first. Regarding the information we store in postgres, there should be no difference between the 1st and 2nd change events.
