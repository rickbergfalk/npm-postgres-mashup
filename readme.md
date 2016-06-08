# npm Postgres Mashup

A cli tool to replecate the npm skimdb into Postgres. 

**Now Utilizing JSONB!**


## New in Version 17

- All packages are captured (even those with unicode NULLs)
- All package data is captured in JSONB document just as it is in npm
- Deleted packages are marked accordingly
- Reporting tables are built out via postgres sql queries and JSONB functions


## Breaking Changes

Version 17 newness brings a lot of breaking changes.

- Complete schema rethink. **ANY PREVIOUS NPM-POSTGRES-MASHUP TABLE WILL BE DROPPED**. If you like the old schema don't upgrade or use a different database for future versions.
- A relational model is no longer used. Instead the couchdb documents are stored in JSONB. 
- npm-postgres-mashup is only intended to be used as a cli utility, npm2pg. The previous api was lacking, and really if you want something to interface with at this level you should just utilize the follow library directly.
- Download counts have been removed. Scraping the download-count api was slow and I felt like things weren't as efficient as they could have been. Download counts might come back but I need to rethink the approach.


## Installation & Usage

```
npm install npm-postgres-mashup -g
```

This installs the cli tool "npm2pg". Next create an empty Postgres database.

Prior to replicating the data into the database, you'll need to build the schema. 
Running this after a major npm-postgres-mashup version change will drop some tables.
To build the schema run:

```
npm2pg -d database -u username -p password --build-schema
```

Once the schema is built, you can start replicating the data. 
Do this by running: 

```
npm2pg -d database -u username -p password
```

npm2pg can also build reporting tables from the package jsonb documents. 
Add ```--reporting-tables``` parameter to run this after sync is complete.
Otherwise you can run the SQL yourself (see ./db/createReportingTables.sql)

To get to the cli help about additional parameters run:

```
npm2pg --help
```

npm-postgres-mashup will automatically add the necessary tables to the postgres database. 



## License

MIT
