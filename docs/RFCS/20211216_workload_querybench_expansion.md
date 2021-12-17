- Feature Name: workload querybench expansion
- Status: draft
- Start Date: 2021-12-01
- Authors: Glenn Fawcett
- RFC PR: 73987
- Cockroach Issue: NA

# Summary

The `querybench` option to the `workload` tool is incredibly useful to test workloads with customer data and queries.  Using their own queries and data are useful, but the existing querybench option only supports the inclusion of fully qualified queries without randomization of predicates.  This makes it difficult to run a real-world workflow.  Additionally, it is also not possible to specify the rate and mix of various queries that make up the customer's workload.

# Motivation

The structure for running, reporting and controlling the workload is already built into the workload binary.  This expansion of `querybench` will allow us to use our embedded tooling to easily reproduce simple workloads running on a customer’s database.  This will also be extremely useful as a way to reproduce test cases and deliver workshops without custom coding or third-party tools.

# Technical design

This RFC is to expand the querybench tool to provide a way to define a full workload to be run based on an existing database schema.  The querybench portion of the workload binary will be expanded to:

- Define the queries with predicates
- Weighting or QPS per query
  - Populate data structure for substitution to be populated by
  - CSV file that contains valid values
- Query to populate valid values
- Provide a Random Seed for reproducible results

For example, a workload could be described by a yaml file like so:

```yml
workload: mytestversion
q1:
   - statement: |
       SELECT count(*)
       FROM mytable
       WHERE id = $1 and other_id = $2
   - csvfile: q1_predicates.csv
   - csvdelimeter: "\t"
   - weight: 80
q2: 
   - statement: |
       SELECT some, stuff
       FROM mytable
       WHERE id = $1 and other_id = $2
   - predicate_array_population_query: |
       SELECT id, other_id from mytable limit 1000 
   - weight: 20
```

## Drawbacks

None that I can identify.

## Rationale and Alternatives

There are several third party tools, like `Jmeter`, that allow for the scripting and simulation of SQL workloads.  While Jmeter is a rich environment that has been used in the industry, it is complex to configure and learn.  There is also an extension called querylog that allows users to replay queries from a text log file.  This new expansion to querybench will allow customers, CAEs, TSEs, and CRL engineering to more easily reproduce customer workloads without relying on external tools or custom coding.

# Explain it to folk outside of your team

Audience: PMs, doc writers, end-users, CockroachDB team members in other areas of the project.

# Unresolved questions

Audience: all participants to the RFC review.
