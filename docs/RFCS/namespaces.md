- Feature Name: SQL namespacing
- Status: draft
- Start Date: 2017-01-31
- Authors: knz
- RFC PR: #13319
- Cockroach Issue:

# Summary

This RFC proposes to fill conceptual gaps in both documentation and
code around how we think about namespacing in SQL.

It is thus a "clean up" RFC more than the addition of new features.

# Motivation

This RFC stems from an unsuccessful attempt of mine at documenting and
explaining how names are looked up in CockroachDB's variant of SQL.

For better or for worse, CockroachDB currently uses some (but not all)
of MySQL's namespacing rules, but combines them with some (but not
all) of PostgreSQL's name resolution rules. Since we do not document
this combination yet, a user wishing to understand how this all makes
sense may look at MySQL docs to understand how names are organized,
then at PostgreSQL docs to understand what a particular SQL query
means, and since these two databases differ widly in how they describe
namespacing, and CockroachDB doesn't really do either of what they do,
major confusion ensues.

So we need to clarify all this.

# Background and methodology

The essence of the issue at hand is that different database engines
use different words for the same things, but also the same words for
different things.

To create some structure the authors of the SQL standard have pushed
most databases to connect what they do to the new "`information_schema`"
abstraction. Unfortunately, this structure is purely abstract and
does not really inform the user about what the various levels
precisely mean and what they can or cannot do.

I found the following questions to be useful instead to create some structure:

- what is the highest level of naming in which a group of tables,
  indexes and user-defined functions or procedures, with a common
  group name, can be used as a single item for the purpose of dumping
  and restoring data?  I'll call this a "logical schema".

- what is the highest level of namepacing in which tables can be renamed transactionally
  (in particular without without needing dump/restore)?
  I'll call this a "transactional schema container".

- what is the namespace of users for the purpose of managing privileges?
  I'll call this the "privilege container".

- what is the highest level of namespacing across which one can
  perform transactional queries? I'll call this the "transactional
  store".

From this structural identification of concepts, one sees that to
preserve the structural semantics the definition entails a *necessary*
hierarchical structure: logical schemas necessarily can't span
multiple transactional schema containers; schema contains necessarily
can't span privilege containers, etc.

# Concept mapping

Using these definitions we can recognize the following conceptual mapping:

| Namespace level      | MSSQL    | PostgreSQL | CockroachDB | MySQL    | SQLLite  | Oracle     |
|----------------------|----------|------------|-------------|----------|----------|------------|
| Transactional store  | (global) | Database   | (cluster)   | Server   | Database | (global)   |
| Privilege container  | Database | Database   | (cluster)   | Database | N/A      | Database   |
| Schema container     | Database | Database   | (cluster)   | Database | Database | Database   |
| Logical schema       | Schema   | Schema     | Database    | Database | Database | DBFile     |

Some points of note:

- MySQL supports cross-database queries, whereas pg
  doesn't. CockroachDB is more similar to MySQL in that regard.
- PostgreSQL supports renaming tables across schemas, whereas
  MySQL can't. CockroachDB is more similar to PostgreSQL in that regard.
- *All mature, enterprise-level database provide separate notions for the schema container
  and the logical schema, including CockroachDB.* (but not MySQL...)
- Yet, of all products that make this distinction, only CockroachDB
  uses the word "database" to call its logical schema abstraction.

For comparison, here's how these various things (currently) map to `information_schema`:

| Namespace level      | MSSQL    | MySQL    | PostgreSQL | CockroachDB |
|----------------------|----------|----------|------------|-------------|
| Catalog              | Database | N/A      | Database   | N/A         |
| Schema               | Schema   | Database | Schema     | Database    |
| Table                | Table    | Table    | Table      | Table       |

# Where we are now

The first take away here is the realization that CockroachDB is
currently very much doing its own thing, and that deserves dedicated
documentation.

Points to drive home:

- CockroachDB currently uses the word "database" for what many other
  SQL products call "logical schema". For all intents and purpose,
  "databases" in CockroachDB are a logical unit of organisation.

- In most SQL products other than CockroachDB, there's a strong
  semantic binding between the intuition of where data is physically
  storage and the word "database". In CockroachDB, the word "database"
  is void of this association. The word that does carry the link to
  the physical storage location is "zone".

- Someone who has learned the word "Database" with MySQL needs to be
  educated about the nuances available in other products, including
  CockroachDB, namely:

  - issues of namespacing are orthogonal to issues of storage. To
	paraphrase, one can have multiple containers for names (e.g. a
	schema) inside a single storage thing, and one can spread out the
	things inside the same name container across multiple storage
	things.

  - in most "larger" database products, the real restrictions about
	what you technically can or cannot do with DDL commands stems from
	the physical storage units / cluster network organization (with
	CockroachDB being one of the least restrictive out there), not
	from the (logical) namespacing that you choose for your
	application.

	**The flip side of the coin is that namespacing-related decisions
	are much more lightweight, and you can adjust your logical design
	at a latter stage much more easily with other things than MySQL.**

This can be concretely enacted as follows:

- explain "how does CockroachDB differ from its competition" in this light;
- provide FAQs relating to these points;
- increase readiness of our (upcoming) support staff to answer questions
  related to these points.

# Possible way forward

From there on one could raise the question of whether we could
*simplify* the messaging / learning curve.

When looking at the table above, consider how far we could go by:

1. renaming the word "database" to "schema" throughout our documentation,
   and make it a synonym in our product, promoting "schema" in error messages, etc.; and
2. create a default schema when initializing an empty cluster, and use that
   schema's name as default lookup namespace when connections don't specify a schema; and
3. start using the word "database" informally for the collection of all schemas
   stored in a cluster, so that users start getting used to this association; and
4. add/update FAQ documents and/or make a larger statement in an appropriate doc page
   about what we mean by "schema" and "database" and make recommendations about why
   this distinction can be useful to application developers.

If we were to do that we would reap the following benefits:

-  the conceptual mapping table is simplified to start overlapping more with competition:

   | Namespace level      | MSSQL    | PostgreSQL | CockroachDB | Oracle     |
   |----------------------|----------|------------|-------------|------------|
   | Transactional store  | (global) | Database   | Database    | (global)   |
   | Privilege container  | Database | Database   | Database    | Database   |
   | Schema container     | Database | Database   | Database    | Database   |
   | Logical schema       | Schema   | Schema     | Schema      | DBFile     |

- the step "creating a database" can be elided from tutorials: create a cluster, create a table, bam, done. *Easier adoption and onboarding.*

- online resources available to users of other SQL products is more transparently reusable with CockroachDB. *Easier mindshare.*

- later on, this vocabulary change would make it easier for us to
  decide a good name when we start supporting different "application
  domains" each with its own set of users / permissions. Such a thing
  could then be simply named "database", keeping the word "cluster"
  for the surrounding container.
