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


## Fun Queries to Run

Once you have npm in Postgres you can find out all sorts of interesting information.

### Total number of distinct publishers

```sql
-- This sort of works but probably isn't exactly accurate
SELECT   COUNT(DISTINCT author_name || author_email)
FROM     package p
JOIN     version v ON p.package_name = v.package_name AND p.version_latest = v.version
ORDER BY COUNT(DISTINCT author_name || author_email) DESC
```

### Most popular version number

```sql
SELECT version, COUNT(*)
FROM version 
GROUP BY version 
ORDER BY COUNT(*) DESC
LIMIT 100
```

### Most popular first version number

```sql
WITH first_version AS (
	SELECT package_name, MIN(time_created) AS first_time
	FROM version
	GROUP BY package_name
)
SELECT v.version, COUNT(*)
FROM first_version fv
JOIN version v ON fv.package_name = v.package_name AND fv.first_time = v.time_created
GROUP BY v.version
ORDER BY COUNT(*) DESC
LIMIT 100
```

### Package with the most versions

```sql
SELECT package_name, COUNT(*) AS version_count
FROM version
GROUP BY package_name
ORDER BY COUNT(*) DESC
LIMIT 100
```

### Oldest Packages

```sql
SELECT * 
FROM package
ORDER BY time_created
LIMIT 100
```

### Average time between version updates

A simple average between versions may not do this data justice, but it's fun right?

```sql
WITH ranked AS (
    SELECT package_name, version, time_created, rank() OVER (PARTITION BY package_name ORDER BY time_created) AS seq
    FROM version
)
SELECT r.package_name, AVG(rnext.time_created - r.time_created) AS avg_time_between_versions
FROM ranked r
JOIN ranked rnext ON r.package_name = rnext.package_name AND r.seq = (rnext.seq - 1)
GROUP BY r.package_name
```

### Version Publish Count by Month

```sql
SELECT date_trunc('month', time_created) AS year_month,  COUNT(*) AS version_publish_count
FROM version
WHERE time_created BETWEEN '01/01/2010' AND '03/01/2014' -- exclude partial month
GROUP BY date_trunc('month', time_created)
ORDER BY 1
```

### Version Publish Count by Hour of day (last 12 months)

```sql
SELECT date_part('hour', time_created) AS hour_of_day,  COUNT(*) AS version_publish_count
FROM version
WHERE time_created BETWEEN '03/01/2013' AND '03/01/2014'
GROUP BY date_part('hour', time_created)
ORDER BY date_part('hour', time_created)
```
