- Feature Name: SQL Query Denylist
- Status: draft
- Start Date: 2020-10-20
- Authors: angelapwen, otan
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [51643](https://github.com/cockroachdb/cockroach/issues/51643)

# Summary

We propose a mechanism built into CockroachDB to prevent operator-defined SQL queries from being executed. This will immediately address the situations where clusters enter unresponsive states due to a query, or a query pattern, that has been identified. To bring the clusters back up as soon as possible and prevent them from going down again due to the same query, we need a query denylist on the database. This RFC details a file-managed SQL query denylist implementation that will allow database administrators to trigger query denial by uploading a YAML file comprising regular expression patterns to a customizable file location.  
# Motivation

It is important to support a file-managed denylist so that database administrators are able to quickly deny queries that have been confirmed to bring down clusters to the point of unresponsiveness. In this situation, SQL commands cannot be run from the console and cluster settings will not be able to be set. 

Without this functionality, there may be applications endlessly retrying the “bad” queries that will continuously bring the cluster down when executed. There is currently no built-in way to stop execution of a query like this other than by preventing the query from being run in the first place, and it is time- and resource-intensive for developers to write their own. 

As blocking a query pattern is invasive and potentially dangerous, it is also important to add a staging denylist for administrators to test whether the query pattern accurately matches the queries they meant to deny or not. 

Having a query denylist will allow database administrators to mitigate these situations quickly and immediately without making any code changes. Additionally, this will give more flexibility and time for CockroachDB’s database and release engineers to work on database fixes relevant to the query. 

# Guide-level explanation

## New named concepts

### Denylist
The denylist we define for this RFC and plan to implement is a global, non-configurable SQL query denylist in CockroachDB. A location for this denylist will be configured via a CLI flag, and if a denylist is uploaded to that location, any following queries matching any pattern on the denylist file will be denied. The supported file format for the denylist is YAML; each query pattern on the denylist file must be specified via regular expression conventions. 

### Staging denylist
The staging denylist is intended to be used so that a user will be able to test whether the query patterns on their denylist deny the appropriate queries, without actually blocking them. A separate location for the staging denylist will be configured via a CLI flag. Otherwise, it should be formatted in the same way as the denylist. 

## Examples

### User experience from a database administrator's point of view
A database administrator, anticipating that a denylist or staging denylist may be used in the future, uses a flag to configure the location where the denylist would be located when it is needed. When they notice a cluster is unresponsive, and isolate the root cause to a particular query (or multiple), they optionally may want to confirm the regex query pattern matches the queries they want to deny. 

To confirm, they can add a staging denylist to the location they had specified for the staging denylist and execute the query. The query will execute, and if the query matches the regex query pattern specified in the denylist, the administrator will be able to view this in the Admin UI, SQL console, and CRDB logs. 

The administrator will then be able to add a denylist to the denylist location specified. Any queries after this moment will be matched against the denylist, and the denial logged for the administrator if there is a match. A time-series graph of queries denied, and queries that would be denied in the case of a staging denylist, will also be available to view on the Admin UI.

The administrator may modify the file content as needed; and if the entire denylist is no longer available, they may remove it entirely. The changes will take effect immediately.

### User experience from a developer's point of view
A developer may be sending a query that has been denied on the denylist or would be denied and is currently on the staging denylist. In the latter case, they will also be able to view the warning on console that this query is still executing, but would be denied due to a pattern match on a staging denylist. In the former case, they will receive an error message that "query matched a pattern in the denylist by the database administrator" with the specific regex query pattern that it matched. 

# Reference-level explanation

## Detailed design

### Design overview
We will implement a file-managed denylist, stored locally on each machine as a YAML configuration file. The exact location of the denylist should be configured with a flag – this will allow users to specify the location of their file, such as via the k8s ConfigMap for CockroachCloud SREs. The file location will be watched so that as soon as the config file is created, modified, or removed, the denylist will be updated. Pattern matching between queries executed and queries on the denylist will be done via regex so users maintain granular control of the queries affected by the denylist.

A staging YAML file will be supported so users will be able to confirm the queries that would be denied: when the staging file is set, users will see logs of the queries that are not yet denied but will be denied if the non-staging denylist file is set.  

### Configuring denylist location
A denylist location flag can be configured in [pkg/cli/flags.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/cli/flags.go). eg. `--denylist=’denylist.yaml’`. If this flag has not been set, then there will be no location to watch for a denylist and therefore the functionality is off by default. 

### File-managed denylist implementation
We expect users to create their own YAML file with a `sql` field that lists an array of strings representing regex to match any SQL queries to be denied. This allows granular control of specific command types as well as database objects — a user should be able to denylist any command of a certain type, any command pertaining to a particular database object, or any additional parameters that may be affecting the cluster.

In the future, we could extend the functionality with additional fields such as, for example, `IP` to support denying queries made from specific IP addresses. 

Example denylist: **denylist.yaml**
```
# Queries to be denied; in the future we may support denying other fields.
sql:
   # Denies the experimental temp tables feature to be turned on. 
   - “SET experimental_enable_temp_tables=on”
   # Denies creating any temporary table.
   - “CREATE TEMP TABLE .*”
   # Denies creating a changefeed for any table.
   - “CREATE CHANGEFEED FOR .*”
   # Denies creating any backups for only database db1.   
   - “BACKUP DATABASE db1 .*”
```

If the denylist location flag has been set, an [fsnotify](https://pkg.go.dev/github.com/fsnotify/fsnotify) watcher will be registered when the server is initialized. fsnotify’s built-in functionality will be used to check if a file in the desired location has been added, modified, or removed. If it has been added or modified, the entire YAML file will be read into a YAML Go Struct:

```
type denylistImpl struct {
   SQL []string `yaml:"sql,flow"`
}
``` 

Each query to be denied from the YAML file will then be updated in real-time in a slice of queries in a newly defined denylist package; every time the file in this location is modified, the entire file will be read again, parsed, and added to the slice of queries. Additionally, every time the location is modified, there will be logging output to track how often the query denylist is modified. 

In the denylist package, this private slice of denylist queries is stored as strings representing regular expressions. The slice of denylist queries needs to be stored in an [`atomic.Value`](https://tip.golang.org/pkg/sync/atomic/#Value) to prevent races against the watcher overwriting the config: each SQL query that comes in must be compared against an atomic slice and further writes to this copy of the slice (from modifications to the denylist YAML file) must be prevented. 

An `IsDenied` method can be exposed to `connExecutor`, so that each query coming through can be checked against any queries in the slice. Note that each time `IsDenied` is called, it will need to load the most recent copy of the slice’s `atomic.Value` before comparing. 

The interface to this file will look like:
```
package denylist

type Denylist interface {
   // Allows connExecutor to check whether a query matches any queries in our data structure.
   SQLIsDenied(query string) bool {
   }

// NewDenylist starts listening to a path for the denylist. 
func NewDenylist(denylistPath) Denylist {
   }
}
```

### Pattern matching with regular expressions
When a query comes in, it will be matched against all the strings of regex in the denylist package [prior to command execution in `connExecutor`](https://github.com/cockroachdb/cockroach/blob/0d2207720729240cec5bc9ac52223cf0350aff28/pkg/sql/conn_executor.go#L1446). A helper function named `checkAgainstDenylist` will be created so that commands of each type, such as Exec Statement, Prepare Statement, etc can all be checked in their individual cases within the switch statement linked above. 

`checkAgainstDenylist` can use the built in `regexp` package to handle the matching. If a match is not discovered, then execution proceeds as normal. 

Once a match is discovered, a `pgerror` will be returned to the user with the appropriate pgcode and information to debug the blocked query,
```
pgerror.WithDetail(
  pgerrors.Newf(
    pgcode.ConfigurationLimitExceeded,
    "query matched a pattern in the denylist by the database administrator",
   ),
   "Matching denylist rule %s", $pattern
)
```

### Metrics logging
When queries are denied, a level 2 event will be logged: `denylist match found: query %s denied, pattern matched %s`. 

Several graphs will be added to the Admin UI, including:
- A graph labeling the number of queries blocked by the denylist, and
- A graph labeling the amount of queries that would be denied when the staging denylist is active

### Staging denylist 
A staging denylist can be configured so that the customer will be able to view which queries would be blocked by the denylist. Its location flag can also be configured in `pkg/cli/flags.go` so that users will be able to set the staging denylist with: `--staging-denylist=’staging-denylist.yaml’`. If this flag has not been used, then there will be no location to watch for a staging denylist and therefore the functionality is off by default. 

The staging denylist and actual denylist should be able to be configured simultaneously. As a result, the queries parsed from the staging denylist should be saved in a separate data structure in the denylist package. For each command, `connExecutor` will first check to block queries in the actual denylist, and then in the staging denylist. 

If a command matches a query on the staging denylist, then it is not in the actual denylist. In this case a level 2 event will be logged: `staging denylist match found: query %s would be denied, pattern matched %s`. Additionally a notice to the console should be surfaced: 
```
params.p.BufferClientNotice(
   params.ctx,
   pgnotice.NewWithSeverityf("WARNING", "query matched a pattern %s in the staging denylist by the database administrator and would be blocked if moved to denylist", $pattern),
)
```
Then this command should be executed as normal. 

## Drawbacks

### Pattern matching with regular expressions
There are a couple of drawbacks to consider due to the use of regular expressions to represent queries to be denied in our YAML file. The first is that it is slightly less user-friendly than exact fingerprint matching, because certain special characters in our queries will need to be escaped: eg. `SELECT \(\(a\)\) FROM b`. We accept this drawback because it gives the user writing the query much more fine-grained control than fingerprint matching does — with fingerprint matching, all queries matching a fingerprint will be blocked and the user would not be able to block, for example, queries for a specific database object.

The second is in performance — regular expression matching in Golang is [notably not performant](https://github.com/golang/go/issues/26623), and in this implementation, when a denylist or staging denylist flag is set, all queries coming through will need to be matched against all queries in the denylist. We accept this performance trade-off because we expect the denylist usage to be few and far between, and not a common use case. We expect a denylist to be active in situations such as: there is a database bug on a specific query type and a fix has yet to be deployed, as well as when a developer has sent too much of a certain query and the user of the denylist must discuss with the developer how they can mitigate. As a result, even though we expect infrequent usage of the denylist, each usage may take anywhere from a day to months in order for the root cause to be mitigated. It should be made clear to a user that when they use the denylist functionality, they are accepting a performance trade-off for stability. 

### Concurrency control on the denylist data structure
The implementation uses `atomic.Value` on the stored slice of denylist queries so that incoming queries are compared against an atomic data structure, preventing a situation where the slice is modified by a `denylist.yaml` file change while it is also simultaneously being compared to the incoming query. Using `atomic.Value` makes a copy of the slice each time an incoming query is compared, which may result in a performance hit. 

To avoid the concurrency control, the alternative would be for the database administrator to restart the server each time the denylist YAML file (or staging file) is modified. As a server restart is a less ideal user experience, this implementation will be a fallback if the performance overhead incurred for the usage of `atomic.Value` or even [`RWMutex`](https://golang.org/pkg/sync/#RWMutex) on the slice of denylist queries is unacceptable. 

## Rationale and Alternatives

### Using YAML vs. other file formats 
Other file formats such as JSON and CSV were considered as well. YAML and JSON were preferred to non-flat lists, as optimization for extensibility was considered: as stated in the File-Managed Denylist Implementation above, in the future additional fields such as IP addresses could be added as keys to the YAML file.

Because regular expression matching was chosen (see Drawbacks section for rationale), it was decided that using regex in JSON and CSV would be unwieldy; additionally, YAML supports comments as in the example YAML file above, whereas JSON does not, and comment functionality for the denylist could prove important for administrators to understand denylist rationale. 

### Regular expression matching pre- vs. post-parsing
The regular expression matching implementation will occur in the `connExecutor`, just prior to command execution. This means that all queries will actually have gone through the parsing stage prior, and then checked against the denylist. While both steps are needed regardless for a query that does not match anything on the denylist, in the case that there is a match, the parsing step will have been extraneous. 

In order to check incoming queries against the denylist before parsing, more significant changes would need to be made in [the interface between pgwire/conn and connExecutor](https://github.com/cockroachdb/cockroach/blob/47a5c7e89384b5cf7a634e9f77d09f8416f16de1/pkg/sql/pgwire/conn.go#L203) — the parsing step would need to be lifted out of the `pgwire` package into the `connExecutor` so that `pgwire` only sends strings in. Then, `connExecutor` would first perform the regex pattern matching against the denylist; and then afterwards the parsing and execution of commands that are not denied.

The current implementation was chosen because the overhead incurred by an extra parsing step is not significant, and a more invasive change to the `pgwire`/`connExecutor` interface was not warranted. Additionally, this overhead would only be incurred in the case of an actual match of a query to the denylist, which should be quite infrequent (much more infrequent than the case of matching all queries against the denylist as discussed in the “Drawbacks” section above). 

### Using file-managed denylist vs. cluster settings/system table
A file-managed system for the denylist was chosen over cluster settings and system table for this first iteration of the denylist. This is because the most urgent use case for our users is a situation in which clusters are entirely unresponsive, and therefore updating a denylist via cluster settings or system table would not be feasible. The file-managed denylist will mitigate the most extreme circumstance and therefore is the first denylist management system we will design and implement.

In the future, it is likely that we will add implementations of the denylist via cluster settings for broader use cases when the cluster is not unresponsive. At this point it would be possible to surface the functionality to the Web UI for an easier user experience.

### Denylist implementation in the database vs. SQL proxy
The concept of a global denylist may make more sense to live in a SQL proxy layer on top of the database, as it is generally not considered part and parcel of core database functionality. Currently we do not support or ship a SQL proxy layer like this on our core database product; we do have one for Cockroach Cloud, but we want to add the functionality to the database because expect that this feature would provide equal value to our on-prem customers. 

The denylist on its own does not seem to warrant the addition of an additional abstraction in the form of a SQL proxy layer to our database product and we consider it out of scope of this RFC. If such a layer were to be added in the future, the denylist functionality should be moved there. 

## Unresolved questions

- **Exactly how much performance degradation will we see with the usage of `atomic.Value` for concurrency control on the data structure representing the denylist? What about with the usage of `RW.Mutex`? Can we accept either of these degradations? — should be resolved through implementation before stabilization via performance benchmarking.** — Expect to be resolved through implementation via performance benchmarking.
- **Exactly how much performance degradation will we see with an active denylist or staging denylist in place? Can we accept this degradation?** — Expect to be resolved through implementation via performance benchmarking.
- **How should we handle the denylist on different nodes considering self-hosted users will have access to this feature? ie. what happens if users upload different denylists on different nodes?** — We consider this operator error, and accounting for this situation is out of scope for this RFC.
- **How does a query denylist work in a multi-tenant world?** — We are not adding different features for multi-tenant use cases; this is a global denylist and is not configurable. Therefore the question is out of scope for this RFC and can be addressed independently if additional configurations are added to the denylist at a later point. 
