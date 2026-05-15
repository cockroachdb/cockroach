- Feature Name: workload querybench expansion
- Status: in-progress
- Start Date: 2021-12-01
- Authors: Glenn Fawcett
- RFC PR: [73987](https://github.com/cockroachdb/cockroach/pull/73987)
- Cockroach Issue: -NA-

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

The yaml file format should begin with `workload:` at the highest level to describe the desired workload.  Each section should describe the attributes of each query.  For the initial phase, the support for CSV files is desired.  The following variables are required to define the queries:

- `qname` :: Name of the query instead of the full text
- `qtxt` :: The text of the query to be run.  Values to be replaced are defined by $1, $2, ... and match the CSV file values
- `csvfile` :: Name of the CSV file containing replacement values for the query
- `csvdelimeter` :: The `","` is default delimeter, but can be specified as anything such as: `"\t"` or `"|"`
- `qweight` :: This is the weighted of this query vs the others defined within the workload

For example, a workload could be described by a yaml file as shown below:

```yml
---
workload:
 -
  qname: Q1
  qtxt:	"SELECT $1::int"
  csvfile:    q1.csv
  csvdelimeter: ","
  qweight: 8
 -
  qname: Q2_tab
  qtxt:	SELECT $1::int, $2::int
  csvfile:    q2.csv
  csvdelimeter: "\t"
  qweight: 1
 -
  qname: Q3
  qtxt:	>-
    SELECT $1::int,
           $2::int,
           $3::int
  csvfile:    q3.csv
  csvdelimeter: ","
  qweight: 1
  ```

## Drawbacks

None that I can identify.

## Rationale and Alternatives

There are several third party tools, like `Jmeter`, that allow for the scripting and simulation of SQL workloads.  While Jmeter is a rich environment that has been used in the industry, it is complex to configure and learn.  There is also an extension called querylog that allows users to replay queries from a text log file.  This new expansion to querybench will allow customers, CAEs, TSEs, and CRL engineering to more easily reproduce customer workloads without relying on external tools or custom coding.

# Explain it to folk outside of your team

Audience: PMs, doc writers, end-users, CockroachDB team members in other areas of the project.

# Unresolved questions

Audience: all participants to the RFC review.
