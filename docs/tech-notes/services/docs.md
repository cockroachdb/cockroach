# Reference / UX docs

See README.md for an overview.

## Concepts

A *web function* is a definition of a computation to happen as a
result of a HTTP request to the CockroachDB service endpoint.

A *service* is a collection of web functions together with their configuration.

A *service rule* is a mapping of a HTTP request path to a web
function, with a path-specific configuration.

A service is defined by:
- a set of zero or more service rules, mapping request paths to web functions.
- a common configuration shared by all the web functions in the service.
- an owner identity, used to execute functions for anonymous HTTP requests.

## Proof of concept (smoke test)

Smallest example (goal):

- Run server with `--enable-services`

- Run using SQL:
  ```sql
  CREATE SERVICE myservice
     USING RULES FROM
     SELECT
         '/hello'      AS path,
         'hello world' AS function,
         'text'        AS exec_method,
         'anonymous'   AS authn_method;
  ```

- Access using HTTP: `curl -k https://localhost:14464/hello`

## Additional examples

We will use the `cockroach gen example-data services` command to
generate example demos so that folk can try the system out.

`cockroach demo` should be extended to demonstrate as well.

## SQL syntax reference

```sql
   CREATE SERVICE myservice
       [WITH DEFAULT OPTIONS (...)]
       [RULES FROM ...];

   DROP SERVICE myservice;

   ALTER SERVICE myservice
	   [RENAME TO ...]
       [SET OWNER TO ...]
       [REFRESH]
       [SET DEFAULT OPTIONS (...)]
       [SET RULES FROM ...]
```

Main syntax elements:

- `RULES FROM ...` takes a selection clause, for example:

  - `RULES FROM TABLE my_rules`
  - `RULES FROM TABLE VALUES (...) AS t(...)`
  - `RULES FROM (SELECT * FROM my_rules JOIN my_config ON ...)`

  It extracts the service configuration data from the query passed after `RULES FROM`. It checks
  the configuration, then *copies* the configuration rules into `system.service_rules`.

  It also stores the query into the service definition, to be used by `ALTER SERVICE REFRESH` later.

- `SET DEFAULT OPTIONS` specifies a set of key/value option pairs to be applied
  as defaults for all the rules in the service.

  This makes it possible to omit common configuration from the rule definitions when all the rules
  share the same config. (Most commonly: `current_database`)

- `ALTER .. REFRESH` runs the pre-defined RULES FROM query and checks the resulting rule definitions.
  The new rules are only saved/installed if the verification succeeds.

## Architecture (how does it work?)

When a node starts up, and periodically after that, it reads rule
definitions from `system.services` and `system.service_rules`.

On startup, the node sets up a HTTP listener and subsequently serves endpoints
according to the rule definitions.

The CREATE/ALTER SERVICE statements validate the configuration before
it is copied to the `system.services` / `service_rules` tables.

We do not constrain the input rules table provided to CREATE/ALTER
... RULES FROM to have a particular SQL schema. In fact, any schema is
acceptable. All the columns are merged together as per `SELECT
to_json(rules.*) ...`.  Unrecognized configuration options are ignored
with a SQL warning.


## Rule configuration options

| Option | Example values | Description |
|--------------------|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `path`             | `/hello`                   | The HTTP path that identifies this service. If the path ends with `/`, any query with a sub-path will be redirected to this endpoint.        |
| `current_database` | `defaultdb`                | Initial value of the `database` session setting, so that SQL queries can omit the DB prefix in table names.                                  |
| `virtual_host`     | `my.service.com`           | Virtual host under which the endpoint is recognized. Default is empty (any vhost includes the endpoint.)                                     |
| `exec_method`      | `text`, `scriggo`, `sql`   | The execution method to use when the endpoint is invoked.                                                                                    |
| `function`         | `hello world`              | The function specification for the endpoint. Syntax/format depends on `exec_method`.                                                         |
| `http_method`      | `get`, `post`, `get,post`  | A comma-delimited list of HTTP methods that are valid for this endpoint.                                                                     |
| `content_type`     | `text/html`                | The value of the `Content-Type` header in the response for this endpoint. If empty, derived automatically from `exec_method`.                |
| `authn_method`     | `none`, `cookie`, `header` | Which authentication method to require when the service is invoked.                                                                          |
| `services_version` | TBD                        | CockroachDB service infra version at which the service was defined. Used to maintain backward-compatibility when the services infra changes. |
| `redirect_ok`      | `/foo`                     | The endpoint to redirect to if a login/logout operation succeeded.                                                                           |
| `redirect_error`   | `/foo`                     | The endpoint to redirect to if an error succeeded.                                                                                           |

## Execution methods

The main goal of Project 80000 is to define a _platform_ on which we
can later add plug-ins and support for more features. We initially
support a couple of execution method to demonstrate the possibilities
but the goal is eventually to delegate the addition of new exec
methods to the community.

Initial execution methods envisioned:

| Exec method              | Description                                                                                                                                                                                          | Example value for `function` | Default `content_type` |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|------------------------|
| `text`                   | The value of `function` is returned as-is.                                                                                                                                                           | `hello world`                | `text/plain`           |
| `scriggo`                | The value of `function` is interpreted as a Scriggo template                                                                                                                                         | `Hello {{ getUser }}`        | `text/html`            |
| `sql` (not impl yet)     | The value of `function` is executed as a SQL query. Query results sent back as JSON. (TBD: query parameters used to feed placeholders)                                                               | `SELECT 'hello world'`       | `application/json`     |
| `table` (not impl yet)   | The value of `function` is a SQL table name. GET queries run SELECT against the table. POST queries run UPSERT.                                                                                      | `mytable`                    | `application/json`     |
| `wasm`  (not impl yet)   | The value of `function` is interpreted as web assembly (TBD: FFI for the wasm sandbox)                                                                                                               | TBD                          | `application/json`     |
| `graphql` (not impl yet) | TBD - we want a way in configuration to restrict the set of tables/columns that the graphql query can access                                                                                         | TBD                          | `application/json`     |
| `login-cookie`           | A pre-defined endpoint definition that recognizes the `username` and `password` query parameters and issues a login cookie back.                                                                     | N/A                          | N/A                    |
| `login-header`           | A pre-defined endpoint definition that recognizes the `username` and `password` query parameters and issues a login token back, to be passed in subsequent requests as `X-Cockroach-Authentication`. | N/A                          | `application/json`     |
| `logout-cookie`          | A pre-defined endpoint definition that closes the session defined by the current session cookie.                                                                                                     | N/A                          | N/A                    |
| `logout-header`          | A pre-defined endpoint definition that closes the session defined by the `X-Cockroach-Authentication` header.                                                                                        | N/A                          | N/A                    |


For the `login`/`logout` exec methods, the browser is redirected via
the value of the options `redirect_ok` / `redirect_error` after
the operation completes.

## Authorization

The execution context of each method is that of the logged-in user (as per the selected `authn_method`).

For queries that can run without authentication (`authn_method =
none`), the execution context is set to the SQL OWNER of the
service. This is:

- the SQL user who ran `CREATE SERVICE`, or
- the value set via `ALTER SERVICE SET OWNER TO` statement,
  whichever happened last.

(TBD: do we want a more fine-grained authorization model?)

## Built-in functions available to `scriggo` templates

- `redirect`: emit a HTTP redirect code and `Location` header in the response.
- `getPath`: retrieve the query path components.
- `getInput`: retrieve a HTTP query or POST form argument.
- `toText`: convert a go JSON object to plain text.
- `getUser`: retrieve the user name of the current user.
- `query`: execute a SQL query and return its results as an iterable.
- `exec`: execute a SQL statement without results.

## Server configuration

By defaults, services are disabled.
Enable via `--enable-services` on the `start` command.

In a first iteration, we only define 1 additional HTTP port for the
services.  The default port number will be 14464. This can be
overridden with `--services-addr` on the command line.

The `--advertise-services-addr` flag is provided for symmetry with
`--advertise-addr`.

In a later iteration we might offer the option to run multiple HTTP
listeners side-by-side.
