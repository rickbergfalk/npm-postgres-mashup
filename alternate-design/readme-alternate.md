# Alternate DB Design

The initial DB design using composite natural keys ended up being a lot larger than I initially expected (580 MB in Postgres vs. 380 MB in CouchDB). 

I attributed this to a lot of the tables having package_name and version repeating quite a bit, which would add up. I also figured that the not-totally-normalized approach might have some impact to all this as well - I'm making no effort to make an author or contributor a single entity, which they would be in a traditional relational database design. 

But I couldn't leave it alone, and I had to pursue some of it out of curiosity. If I replace the natural composite keys (package_name, version, and keyword) with surrogate integer keys (package_id, version_id, and keyword_id) how much space would I save? 

The answer? Not as much as I thought.

The surrogate key design still weighed in at around 440 MB. It shaved off 140 MB, but still came in higher than CouchDB by 60 MB. 

## Can we do better?

There were other normalization and optimization techniques available, that I almost pursued, but then realized that it would be a severely complex to do so.

Some of why that is stems from the flexibility and schemaless-nature of using a document store like CouchDB. Other reasons stem from poor data quality or incomplete data. 

For example, when specifying your dependencies in a package.json file, you aren't required to specify an exact version of that dependency. In fact, you're almost encouraged to specify something a bit more loose that maps close to a certain patch version. I'm not going to go into all the semantic versioning stuff here, but it provides a difficult scenario to model relationally. ```version_id``` represented a package at a certain version, and we couldn't just make a table consisting of ```version_id``` and ```dependency_version_id``` (although, that's kind of what I was wanting to do). 

Now we could totally do that, but it gets complicated when new packages are published. Whenever a module is published with a new version, we'd need to go through and update all the modules that depend on it. 

Another tricky scenario is modeling contributors and maintainers. Ideally, I suppose we'd have a person table that looks something like this:

```sql
CREATE TABLE Person (
	person_id   INT PRIMARY KEY,
	name        TEXT,
	email       TEXT
);
``` 

Then, the ```version_maintainer``` table would look like:

```sql
CREATE TABLE version_maintainer (
    version_id             INT,
    maintainer_person_id   INT,
	FOREIGN KEY (version_id) REFERENCES version (version_id),
    FOREIGN KEY (maintainer_person_id) REFERENCES person (person_id)
);
```

The problem with this approach though is that because of the document storage, the same maintainer or contributor could be specified in multiple places differently. In one module, maybe they have their name and workplace email, another module just their name, and in another just their email. And maybe in another their name and personal email. 

Constraints like this don't really matter to npm and a document-store, so it would be hard and potentially unwise to force this in a relational model.  

It was about this point I decided to stop pursuing this kind of relational model for this npm-postgres-mashup experiment. The further we stray from the initial design, the more unpleasant and impractical it becomes to work with npm data in this kind of data model. 