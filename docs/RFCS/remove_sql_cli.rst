:Feature Name: Supersede the SQL command-line client with psql
:Status: draft
:Start Date: 2016-02-01
:Authors: knz
:RFC PR: (PR # after acceptance of initial draft)
:Cockroach Issue: #4052, #4016, #4018, #4017, #3985, perhaps #3529

Summary
=======

Reduce/demote support for ``cockroach sql``.

Advertise ``psql`` (from the package ``postgresql-client``)
or possibly ``pgcli`` (from https://github.com/dbcli/pgcli ) instead.

Keep minimal own CLI support for marketing.

Motivation
==========

``cockroach sql`` has severe limitations currently, and not all can be
solved within CockroachDB:

- only one result set is returned for a query containing multiple
  statements (#4016). This is actually caused by a limitation in the
  interfaces offered by Go's standard ``sql`` module.

- update/insert do not report the number of rows affected.

- the output format is not configurable.

Since this client talks over pgwire anyways, it seems obvious to consider
using a native command-line PostgreSQL client instead, for example
the official client ``psql`` from ``postgresql-client`` or ``pgcli`` from https://github.com/dbcli/pgcli .

Pros:

- solves the existing issues in CockroachDB's own client (#4016,
  #4052, #4018, #4017);

- automatically brings in numerous psql / pgcli features:
    
  - provides customizable output formatting (including HTML; see
    #3985);
  - adds support for colors in interactive prompts!
  - (psql) support per-client preconfigured statements (eg ``:mystatement``
    configured in ``~/.psqlrc``)
  - (psql) add support for downloading a table (``\copy``)
  - many more! see
    http://www.postgresql.org/docs/9.4/static/app-psql.html
    https://github.com/dbcli/pgcli
  
- reduces CockroachDB's code base: less code to maintain, test, debug,
  etc.
- brings us fast closer to Beta (no need to spend dev time on sql cli)
    
Cons:

- at least in the beginning, the ``\d`` meta-commands from psql would not
  work and fail with non-intuitive error messages. This can be
  mitigated by appropriate documentation, though.

- using psql would tie CockroachDB to PostgreSQL slightly, by requiring
  CLI SQL users to install the PostgreSQL client package.

  (Note that devs using cockroach in their applications would probably already
  use the pg driver anyway, so this Cons only pertains to CLI users.)

Detailed design
===============

- update the documentation to advertise compatibility with / use of
  ``psql`` or ``pgcli``;
  
- remove the code for the interactive CLI; keep only minimal code to
  execute a single statement, print its results then exit, and
  use that in our own sql tests;

- extend the test suite with SQL testing using ``psql`` or ``pgcli``;
  
- eventually, if using psql submit a PR to the PostgreSQL team to
  enable the redefinition of meta-commands by means of
  configuration, and provide a configuration of meta-commands
  suitable for use with CockroachDB.

    
Drawbacks
=========

TBD?

Alternatives
============

Keep the current custom client and deal with its limitations (NIH syndrome?).

Unresolved questions
====================

None?
