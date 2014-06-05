# Readme Too

More information about the npm-postgres-mashup


## Database Design Considerations

I had a couple goals in mind when transitioning npm data to the relational model. I wanted this database to be easy to use and query. I also wanted it to capture most of the npm data, yet still retain some of the flexibility of what a document store might offer (unlimited text length, free-formy text). 

To create an enjoyable data model, I opted to use **natural composite keys** wherever possible. In other words, there are no artificial surrogate keys such as auto-incrementing integers or random UUIDs. For the table design, this meant that package name and version number would be available in almost every table.

The benefit of going this route is a clean data model easy to read and query. When writing report-style select queries it cuts down on some of the JOINs one might have used, since package name and version number are readily available. Not that npm is big enough for this sort of stuff to make a big impact, but I found it pleasant to use.

One disadvantage to this approach however was that a natural primary key was not always available. Some tables in this mashup don't have primary keys, a relational no-no. This is out of laziness on my end and on npm, as some values are allowed to be repeated (like keywords, contributors, etc.)

Another disadvantage (that took me by surprise) was the increased database size. Package names and version numbers often are repeated, inflating table size. My local npm skimdb in couch was roughly 380 MB, which became 580 MB in postgres. (Out of curiosity I did explore using surrogate integer keys in the alternate design , but the savings weren't worth the ugliness. [see alternate-design](alternate-design/readme-alternate.md))

In reality though, 580 MB is rather small and easily workable.

In order to keep the Postgres data model flexible, Postgres' TEXT datatype was heavily used. npm on CouchDB doesn't enforce any max value length that I'm aware of, and [Postgres' TEXT datatype doesn't have any additional overhead than a VARCHAR datatype](http://blog.jonanin.com/2013/11/20/postgresql-char-varchar/). As a SQL Server person, that last detail caught me off guard, as using TEXT for everything would be a quick way to kill your SQL Server performance.

Speaking of questionable relational database practices, this mashup also utilizes CASCADING DELETEs. If you delete a package from the package table, all the related records will be removed. This makes sense in our scenario since we aren't fully normalizing our data. A particular contributor or maintainer will be listed in their respective table multiple times because we aren't trying to formalize and normalize what an individual contributor or maintainer is. We're still treating our data as a document in a way, but it just happens to be spread out across several tables. 


