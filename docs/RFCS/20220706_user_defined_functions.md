- Feature Name: User-Defined Functions
- Status: in-progress
- Start Date: 2022-06-07
- Authors: Marcus Gartner, Chengxiong Ruan, Andrew Werner, Oliver Tan
- RFC PR: [#83904](https://github.com/cockroachdb/cockroach/pull/83904)
- Cockroach Issue: [#58356](https://github.com/cockroachdb/cockroach/issues/58356)

## Summary

User-defined functions, or UDFs, are named functions defined at the database
layer that can be called in queries and other contexts. UDFs allow users to
compose complex queries from reusable functions and to bring computation to data
to reduce round-trips. Several popular tools, including
[Hasura](https://github.com/hasura/graphql-engine) and
[PostgREST](https://github.com/PostgREST/postgrest), rely on UDFs. Furthermore,
UDFs are a stepping stone toward implementing triggers.

The path to feature-parity with Postgres UDFs is arduous because of the
flexibility Postgres allows in creating and using UDFs. In Postgres, nearly all
types of statements are allowed within a UDF body. A UDF can mutate data with
INSERT/UPDATE/DELETE statements, make schema changes, set session settings, call
other UDFs, and recursively call itself. Functions can be written in several
different languages including SQL, PL/pgSQL, PL/Tcl, PL/Perl, PL/Python, and C.
UDFs can be called in a variety of contexts like WHERE clauses, FROM clauses,
CHECK constraint expressions, and DEFAULT expressions. To safely give users the
same level of freedom, we must prudently consider and test the complex
interactions between all these features and UDFs.

This RFC proposes an initial implementation of UDFs. It lays down fundamental
machinery and execution support for basic SQL UDFs. It does not cover in detail
many features of UDFs provided by Postgres, and many of the freedoms described
above will be prohibited initially. The specific features we aim to initially
support are detailed in the [Features and Syntax](#features-and-syntax) section,
and some of the remaining features are discussed in [Future Work](#future-work).

## Features and Syntax

All UDFs have several basic components including a name, a list of arguments and
their types, a return type, a function body and a language. For example, below
is a UDF that returns the sum of two integers:

```sql
CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$
  SELECT a + b;
$$;
```

Function arguments have a type, mode, and optional default value. The type can
be a built-in type, a composite type, an implicit record type (see
[#70100](https://github.com/cockroachdb/cockroach/pull/70100)), a reference to
the type of a table's column (with the syntax `table_name.column_name%TYPE`), or
an arbritrary data type (see [Postgres's documentation on polymorphic
functions](https://www.postgresql.org/docs/current/xfunc-sql.html#XFUNC-SQL-POLYMORPHIC-FUNCTIONS)).
**We will initially support only built-in types, user-defined enums, and
implicit record types.** Argument modes include the default `IN`, `INOUT`,
`OUT`, and `VARIADIC`. The "out" variants mark an "argument" as a return
parameter and cannot be used when specifying a `RETURNS` type. `VARIADIC` allows
defining a function with variable arity. **We will initially support only the
`IN` argument mode.** `DEFAULT` values for arguments can be specified for `IN`
and `INOUT` arguments. **We will not initially support `DEFAULT` values for
arguments.**

`RETURNS` specifies the return type of the function, which can be a built-in
type, a composite type, or a reference to the type of a table's column. The
`SETOF` modifier allows returning multiple rows rather than a single value. The
`TABLE` modifier allows returning a table with multiple columns and rows.
`RETURNS VOID` indicates that there is no return type and `NULL` will always be
returned. **We will initially support returning built-in types, user-defined
enums, implicit record types, `SETOF`, and `VOID`.** We will be able to support
`TABLE` once we are able to support user defined record types (tracked by [issue
27792](https://github.com/cockroachdb/cockroach/issues/27792)).

The function body can reference arguments by name, or by their ordinal in the
function definition with the syntax `$1`, like in a prepared statement. If the
return type of the function is not `VOID`, the last statement of a UDF must be a
`SELECT`, or an `INSERT`, `UPDATE`, `UPSERT`, or `DELETE` with a `RETURNING`
clause. Postgres allows most types of statements within a UDF body. Notable
exceptions are transaction control commands like `COMMIT` and `SAVEPOINT`, and
utility commands like `VACUUM`. **Our initial implementation will support
`SELECT` statements, and we hope to support mutations (`INSERT`, `UPDATE`,
`UPSERT`, `DELETE`). Until we can address [a known issue that also affects CTEs
with mutations](https://github.com/cockroachdb/cockroach/issues/70731), we will
not allow the same table to be mutated from multiple statements in a UDF, in
multiple UDFs, or in the calling statement and a UDF. We will not initially
support other statements including schema changes (see [DDL (Schema Changes) in
UDFs](#ddl-schema-changes-in-UDFs)), session settings, or creating table
statistics.**

Postgres allows UDFs to be invoked in many contexts including `SELECT` clauses,
`WHERE` clauses, `FROM` clauses, and table expressions (`DEFAULT` column
expressions, computed column expressions, partial index predicates, and
expression indexes). **We will initially support invoking UDFs only in `SELECT`,
`FROM`, and `WHERE` clauses of DML statements.**

Initially we will disallow recursive functions and function cycles (see
[Recursion and Function Cycles](#recursion-and-function-cycles)).

Postgres supports writing UDFs in [a handful of procedural
languages](https://www.postgresql.org/docs/current/xplang.html). **We will
initially support only SQL UDFs.**

Other options that Postgres supports are shown below. Only a few will be
initially supported.

Field                                                                 | Initial Support? | Details
--------------------------------------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
`TRANSFORM [ FOR TYPE ]`                                              | No               | Transforms are used to convert between SQL types and procedural languages types.
`WINDOW`                                                              | No               | Indicates that C function is a window function.
`IMMUTABLE \| STABLE \| VOLATILE`                                     | Yes              | Indicates the volatility of a function (see [`Volatility.V`](https://github.com/cockroachdb/cockroach/blob/928f605c3e84efb30cab213b47b2f4a66b816701/pkg/sql/sem/volatility/volatility.go#L15-L62)). The default is `VOLATILE`. Functions with side-effects must be `VOLATILE` to prevent the optimizer from folding the function. A `VOLATILE` function will see mutations to data made by the SQL command calling the function, while a `STABLE` or `IMMUTABLE` function will not (see the [Optimization and Execution](#optimization-and-execution) section and [Appendix A](#appendix-a) for more details).
`[ NOT ] LEAKPROOF`                                                   | Yes              | Indicates that a function has no side effects and that it communicates nothing that is dependent on its arguments besides its return value (e.g., it cannot throw an error). We currently consider `LEAKPROOF` as a separate type of volatility, rather than a volatility qualifier. Postgres allows `STABLE LEAKPROOF` functions, but we do not currently have the ability to represent this volatility. Until we decouple `LEAKPROOF` from volatility, we will only allow `LEAKPROOF` with `IMMUTABLE` which will map to [`Volatility.Leakproof`](https://github.com/cockroachdb/cockroach/blob/928f605c3e84efb30cab213b47b2f4a66b816701/pkg/sql/sem/volatility/volatility.go#L45). `NOT LEAKPROOF` will be allowed with any volatility and have no effect.
`CALLED ON NULL INPUT \| RETURNS NULL ON NULL INPUT \| STRICT`        | Yes              | `CALLED ON NULL INPUT` indicates that the function will be executed if any of the arguments is `NULL`. `RETURNS NULL ON NULL INPUT` or `STRICT` indicates that the function always evaluates to `NULL` when any of its arguments are `NULL`, and it will not be executed. This maps to [`NullableArgs`](https://github.com/cockroachdb/cockroach/blob/94fe056cbc7f1245266ca7ea293650b370ee239a/pkg/sql/sem/tree/function_definition.go#L51-L65). We will likely need to move `NullableArgs` from `FunctionProperties` to `Overload` because it is specific to an overload.
`[ EXTERNAL ] SECURITY INVOKER \| [ EXTERNAL ] SECURITY DEFINER`      | No               | Indicates the privileges to run the function with. The default is `SECURITY INVOKER` which means that the function is executed with the privileges of the user that invokes it. We will not initially support `SECURITY DEFINER`.
`PARALLEL [ UNSAFE \| RESTRICTED \| SAFE ]`                           | No               | Indicates whether or not the function is safe to execute in parallel. The default is `PARALLEL UNSAFE` which means that the functions cannot be executed in parallel. This is the only option we will initially support (see the [Optimization and Execution](#optimization-and-execution) section for why distribution of non-inlined UDFs is not possible).
`COST execution_cost`                                                 | No               | The estimated execution cost of a function that aids in query optimization. Functions that are inlined (see the [Optimization and Execution](#optimization-and-execution) section) will not need this estimate. Before we can support this, we'll need to decide on a unit that makes sense to users.
`ROWS result_rows`                                                    | No               | The estimated number of rows returned by the function that aids in query optimization. Only allowed when the function returns a set. The default is 1000 rows. Functions that are inlined (see the [Optimization and Execution](#optimization-and-execution) section) will not need this estimate.
`SUPPORT FUNCTION`                                                    | No               | Allows use of a [planner support function](https://www.postgresql.org/docs/current/xfunc-optimization.html), written in C, that supplies additional information to the optimizer to help with query planning.
`SET configuration_parameter { TO value \| = value \| FROM CURRENT }` | No               | Allows a session variable to be set for the duration of the function.
`sql_body`                                                            | No               | Allows writing the function body as an expression rather than a string constant. It behaves differently to the string constant form. It only is allowed for SQL functions and dependencies of objects referenced in the function are tracked, which prevents a table, column, function, etc. rename from breaking a function or schema. The [Postgres docs](https://www.postgresql.org/docs/current/sql-createfunction.html) note that "this form is more compatible with the SQL standard and other SQL implementations."

## Resolution

Resolution contains two steps: `Function Resolution` and `Overload Resolution`.
The goal is to return the best matched function overload given a function
expression. Simply put, Function Resolution returns a list of candidate
overloads given a function name and current search path, then Overload
Resolution filters the candidate overloads down to one single overload given the
function expression arguments and an optional return type. Function Resolution
can be used to access function properties, and Overload Resolution is performed
to determine the type of a function expression. CRDB currently has an
implementation of function resolution logic and overload filtering heuristics
for builtin functions. However, with user defined functions introduced, things
are getting a little bit more complicated.

Note: It seems weird to distinguish the word `function` from `overload`. They
“are” different concepts and it helps to draw the line between the two steps
above. But it also makes it feel like there’s a hierarchy that a function
represents a group of overloads. Just want to clarify that there is just one
level representation: overloads. Moving forward, the two words might be
mix-used. But just keep in mind that the goal is get one single overload.

There will be two paths of resolutions:

### I. Builtin Function Resolution

Only builtin functions are resolved in this path. This is needed to satisfy
current usage of `tree.WrapFunction` function which assumes the input string is
a builtin function name. It’s important because there are quite a bunch of SQL
statements which are parsed directly into builtin functions calls. So we need
this as a shortcut for builtin function resolution. Namely, in this path, no
user defined functions are considered. It is also required by CDC filters which
only consider builtins and CDC specific overrides.

### II. General Function Resolution

The desired behavior is:

#### A. All functions on the search path are considered.

This includes both builtin functions and UDFs.

#### B. Only functions in the qualified name are considered.

Given a qualified function name, only functions in the specified schema are
considered. This is also true for builtin functions under `pg_catalog` schema.

#### C. Consider all schemas for unqualified names.

If an unqualified function name is provided, all schemas on the current
`search_path` will be searched for the function and the earliest best matched
qualified function is returned. That is:

1. If there are multiple functions with an identical signature on the search
   path that match the target, the earliest one found is returned. This implies
   that builtin functions are shadowed if `pg_catalog` schema is specified in
   the current search path but there’s a function with identical signature. On
   the flip side, builtin functions win if `pg_catalog` is not in the search
   path.

   ```
   Example (1): earlier schema in search path wins
   Target Function: f(TypeA) returns TypeB
   Search Path: sc1, sc2
   Candidates in schema sc1: f(TypeA) returns TypeB
   Candidates in schema sc2: f(TypeA) returns TypeB
   Candidates in pg_catalog: None
   Resolution result: sc1.f
   ```

   ```
   Example (2): pg_catalog builtin wins when not specified in search path
   Target Function: f(TypeA) returns TypeB
   Search Path: sc1
   Candidates in schema sc1: f(TypeA) returns TypeB
   Candidates in pg_catalog: f(TypeA) returns TypeB
   Resolution result: pg_catalog.f
   ```

   ```
   Example (3): pg_catalog builtin is shadowed when it’s later in search path
   Target Function: f(TypeA) returns TypeB
   Search Path: sc1, pg_catalog
   Candidates in schema sc1: f(TypeA) returns TypeB
   Candidates in pg_catalog: f(TypeA) returns TypeB
   Resolution result: sc1.f
   ```

2. Similar to (1), but function signatures are not identical, however they
   equally match the target, the earliest match is returned.

   ```
   Example (1): earlier schema in search path wins
   Target Function: f(TypeA) returns TypeB
   Search Path: sc1, sc2
   Candidates in schema sc1: f(TypeC) returns TypeB // TypeC constant can become
   TypeA
   Candidates in schema sc2: f(TypeD) returns TypeB // TypeD constant can become TypeA, and equally important as TypeC based on CRDB’s heuristics.
   Resolution result: sc1.f
   ```

3. Always return the best match even if there’s a compatible match earlier in the search path.

   ```
   Example (1): Better match in later schema wins
   Target Function: f(TypeA) returns TypeB
   Search Path: sc1, sc2
   Candidates in schema sc1: f(TypeC) returns TypeB // TypeC constant can become TypeA
   Candidates in schema sc2: f(TypeA) returns TypeB // Note that this is a exact match
   Candidates in pg_catalog: None
   Resolution result: sc2.f // It’s the better match even though sc1.f is compatible.
   ```

   ```
   Example (2): Better match in same schema wins
   Target Function: f(TypeA) returns TypeB
   Search Path: sc1,
   Candidates in schema sc1: f(TypeC) returns TypeB, // TypeC constant can become TypeA
   f(TypeA) returns TypeB,
   Candidates in pg_catalog: None
   Resolution result: sc1.f(TypeA)
   ```

4. Ambiguity Error is returned if there are multiple equally matched candidates
   in the earliest schema on the search path. That is, we failed to narrow down
   to one single overload.

5. Note that all functions in `crdb_internal` are exceptions here. They need to
   be called with a qualified function name. Namely, function name should be
   always prefixed with the `crdb_internal` schema name.

CRDB’s current implementation of function overload resolution considers all
overloads equally beginning with stricter heuristics (e.g., input argument types
exactly match overload argument types ) followed by relaxed heuristics (e.g.,
input arguments can be casted to match overload argument types). It tries to
find the best match. However, the position of the schema in the search path is
not considered. We may reuse this logic, but extend it to also  check schema
position.

One thing to note is that, currently there are multiple places in CRDB trying to
access function properties to make decisions about next steps. And in some cases
function expressions are not type checked yet. That is, only the `Function
Resolution` step is performed and we only assume that there is a group of
overloads which share function properties, so that it’s safe to use it even if
these overloads are not resolved and narrowed down to a single choice. This is
fine when we only have builtin functions, but this is not true anymore with
UDFs. We want to throw an ambiguity error here as well if there’s any function
properties conflicts among the overload candidates.

Based on the fact that UDFs can have the same name as tables/schema/databases,
we choose not to give UDFs namespace entries. Namely, we won’t write any record
into the `system.namespace` table. Instead, we will store UDF signatures in the
schema descriptor, so that name resolution can be done efficiently without
reading the `system.namespace` table. To achieve this, a mapping from function
name to a list of overload signatures, which includes argument type OIDs, return
type OIDs, and UDF descriptor IDs, will be stored in the schema descriptor (see
the [Descriptors](#descriptors) section below). Schema descriptor versions need
to be bumped when adding/removing/renaming UDFs to make sure future function
resolution works properly with the new UDF set. With that, UDF resolution is as:

1. Get the whole list of overloads with a function name from schemas on the
   search path.
2. Apply overload filter heuristics on the overload list to narrow it down to
   one single overload.
3. If it’s a UDF overload, we get the descriptor id and we can simply get the
   UDF descriptor (which contains all metadata about the overload) by id.

This also implies a plan that UDF descriptors will skip the `by name` cache in
all layers of the descriptor collection which act as a descriptor cache for each
session. The `by name` cache is used to fetch a descriptor by name quickly in a
same session. It is indexed by namespace entry `(parent_db_id, parent_schema_id, name)`.
Since there won't be any namespace entry for UDF, we shouldn't write to this
cache. Writing to this cache would also cause conflicts with tables within a
same schema if they share a name.

These should be good enough to retrieve UDF descriptors. However a UDF
descriptor is not enough for function expression evaluation based on Cockroach’s
current implementation. We need to translate the UDF descriptor into a
`tree.FunctionDefinition` and hydrate any user defined types. This means that a
cache of translated and hydrated UDF definitions is needed for fast name
resolutions. We’ll need to generalize the existing `hydratedtables.Cache` or add
something similar to it.

## Reference Tracking

Reference tracking is important for correctness. Postgres 14 added a new
`sql_body` feature which requires a UDF function body to be stored as an AST
with referenced objects annotated with OIDs. This allows objects referenced in
the UDF body to be renamed without breaking the UDF. CRDB currently doesn’t have
the same infrastructure or similar to guarantee such level of correctness. In
the short term, we propose to disallow renaming objects referenced in the UDF
body. Such that, in the first version, reference tracking includes:

1. Any object (table, type, UDF and etc.) referenced in the UDF function body
   will be tracked. It will be tracked at the granularity of which column/index
   is referenced in the function body. This tracking information, at least in
   the first pass of UDF, will be used to block renaming or altering the type of
   the referenced objects.
2. Usage of a function in tables/views will be tracked. It will be tracked at
   the granularity of a UDF used in which column/index/constraint of a table.
   This is important to not violate constraints and correctness of indexes.
3. Usage of user defined types within UDF definition will be tracked.
4. We won't allow any kind of cross-database references in UDF or referencing
   a UDF in a different database from any object.

Examples:

1. Outbound reference

   With UDF definition `f`:
   ```sql
   CREATE FUNCTION f() RETURNS u_enum LANGUAGE SQL AS $$
       SELECT a FROM t@idx
   $$
   ```
   We’ll record in table `t`’s descriptor that column `a` and index `idx` are
   referenced by UDF `f`. In `f`’s descriptor, we’ll record that `f` depends
   on table `t` and type u_enum.


2. Inbound reference

   With table definition `t`:
   ```sql
   CREATE TABLE t (
     id INT PRIMARY KEY,
     a INT NOT NULL CHECK (f(a) > 100), -- this creates a check constraint “check_a”
     b INT NOT NULL DEFAULT f(a),
     INDEX idx (a) WHERE f(a) > 100
   )
   ```
   We’ll record in table `t`’s descriptor that it depends on UDF `f`. In
   `f`’s descriptor, we’ll record that `f` is referenced by column `b`,
   constraint `check_a` and index `idx` of table `t`.

Reference tracking requires objects to be resolved and type checked at function
definition time which happens after SQL statements are fully understood by CRDB.
Currently these are done by `optBuilder`. One existing example is the `CREATE
VIEW` statement which also requires the view query to be understood and
referenced objects in it to be resolved, it goes through `optBuilder` which
builds all view dependencies. Similarly the `CREATE FUNCTION` statement will
flow through the same path as `CREATE VIEW` instead of the vanilla `planOpaque`
path.

Reference tracking will also be used to block `DROP STRICT` operations on
objects referenced by UDF or UDF referenced by other objects. It's also useful
as a dependency graph when executing `DROP CASCADE` on a UDF or other objects,
so that a column/index can be dropped properly when a referenced UDF is dropped
and a UDF is dropped properly when a table referenced in the body is dropped.

Binding references to tables, view, other functions, types, etc. in the function
body at definition-time will require rewriting function bodies to use fully
qualified names. This will prevent referenced objects from being renamed, but we
plan to lift this limitation in the future by rewriting references as OIDs
instead of fully qualified names (see the [Function Body
Rewrite](#function-body-rewrite) section in Future Work).

The early-binding of references in function bodies is a deviation from
Postgres's behavior in which references in function bodies are resolved at
execution-time (except when a function body is defined with the non-string
`sql_body` syntax). We differ from Postgres here because late-binding makes
users vulnerable to index corruption (see [Appendix D](#appendix-d)). However,
we realize that some users transitioning from Postgres will rely on late-binding
semantics, so we will add a `udf_early_binding` session setting, defaulted to
"on", that can be set to "off" to disable reference tracking and rewriting
references in function bodies as fully-qualified names.

## Permissions

`Function` privilege type will be introduced and `EXECUTE` privilege will be
added. And UDF will follow the following rules:

1. User should have `CREATE` privilege on a schema to create a function under
   it.
2. User should have at least `USAGE` privilege on a schema to resolve functions
   under it.
3. User should have `EXECUTE` privilege on a function to call the function.
4. User should have `USAGE` privilege on a user defined type to define a
   function with the type.
5. User should have privileges on all the objects referenced in the function
   body at execution time. Privileges on referenced objects can be revoked and
   later function calls can fail due to lack of permission.
6. `EXECUTE` privilege can be granted at database level as default privileges
   and new function created inherit privileges from the database level.

`SHOW GRANTS ON FUNCTION` support will be added for UDFs to list out current
UDF privileges. And `information_schema.role_routine_grants` table needs to be
populated properly to be compatible with postgres.

## Descriptors

```protobuf
message SchemaDescriptor {
  // ... other fantastic fields of schema descriptor

  message FunctionOverload {
    option (gogoproto.equal) = true;

    optional uint32 id = 1 [(gogoproto.nullable) = false,
      (gogoproto.customname) = "ID", (gogoproto.casttype) = "ID"];

    repeated uint32 arg_types = 2 [(gogoproto.nullable) = false, (gogoproto.casttype) = "github.com/lib/pq/oid.Oid"];
    
    optional uint32 return_type = 3 [(gogoproto.nullable) = false, (gogoproto.casttype) = "github.com/lib/pq/oid.Oid"];
    
    optional bool return_set = 4 [(gogoproto.nullable) = false];
  }
  message Function {
    option (gogoproto.equal) = true;

    optional string name = 1 [(gogoproto.nullable) = false];
    repeated FunctionOverload overloads = 2 [(gogoproto.nullable) = false];
  }

  repeated Function functions = 13 [(gogoproto.nullable) = false];
  // Next field is 14.
}

message FunctionDescriptor {
  option (gogoproto.equal) = true;
  // Needed for the descriptorProto interface.
  option (gogoproto.goproto_getters) = true;

  enum Volatility {
    Immutable = 0;
    Stable = 1;
    Volatile = 2;
  }

  enum NullInputBehavior {
    CalledOnNullInput = 0;
    ReturnsNullOnNullInput = 1;
    Strict = 2;
  }

  enum Language {
    Sql = 0;
  }

  message Argument {
    option (gogoproto.equal) = true;

    enum Class {
      In = 0;
      Out = 1;
      InOut = 2;
      Variadic = 3;
    }
    optional Class class = 1 [(gogoproto.nullable) = false];
    optional string name = 2 [(gogoproto.nullable) = false];
    optional sql.sem.types.T type = 3;
    optional string default_expr = 4;
  }
  optional string name = 1 [(gogoproto.nullable) = false];
  optional uint32 id = 2 [(gogoproto.nullable) = false,
    (gogoproto.customname) = "ID", (gogoproto.casttype) = "ID"];

  optional uint32 parent_id = 3 [(gogoproto.nullable) = false,
    (gogoproto.customname) = "ParentID", (gogoproto.casttype) = "ID"];
  optional uint32 parent_schema_id = 4 [(gogoproto.nullable) = false,
    (gogoproto.customname) = "ParentSchemaID", (gogoproto.casttype) = "ID"];

  repeated Argument args = 5 [(gogoproto.nullable) = false];

  message ReturnType {
    option (gogoproto.equal) = true;
    optional sql.sem.types.T type = 1;
    // Whether it is returning a set of values
    optional bool is_set = 2 [(gogoproto.nullable) = false];
  }
  optional ReturnType return_type = 6 [(gogoproto.nullable) = false];;
  optional Language lang = 7 [(gogoproto.nullable) = false];
  // Queries in this string are rewritten with fully qualified table names.
  optional string function_body = 8 [(gogoproto.nullable) = false];

  optional Volatility volatility = 9 [(gogoproto.nullable) = false];
  optional bool leak_proof = 10 [(gogoproto.nullable) = false];
  optional NullInputBehavior null_input_behavior = 11 [(gogoproto.nullable) = false];
  optional PrivilegeDescriptor privileges = 12;

  repeated uint32 depends_on = 13 [(gogoproto.casttype) = "ID"];
  repeated uint32 depends_on_types = 14 [(gogoproto.casttype) = "ID"];

  message Reference {
    option (gogoproto.equal) = true;
    // The ID of the relation that depends on this function.
    optional uint32 id = 1 [(gogoproto.nullable) = false,
      (gogoproto.customname) = "ID", (gogoproto.casttype) = "ID"];
    // If applicable, IDs of the inbound reference table's index.
    repeated uint32 index_ids = 2 [(gogoproto.nullable) = false,
      (gogoproto.customname) = "IndexIDs", (gogoproto.casttype) = "IndexID"];
    // If applicable, IDs of the inbound reference table's column.
    repeated uint32 column_ids = 3 [(gogoproto.customname) = "ColumnIDs",
      (gogoproto.casttype) = "ColumnID"];
    // If applicable, IDs of the inbound reference table's constraint.
    repeated uint32 constraint_ids = 4 [(gogoproto.customname) = "ConstraintIDs",
      (gogoproto.casttype) = "ConstraintID"];
  }
  repeated Reference depended_on_by = 15 [(gogoproto.nullable) = false];

  optional DescriptorState state = 16 [(gogoproto.nullable) = false];
  optional string offline_reason = 17 [(gogoproto.nullable) = false];
  optional uint32 version = 18 [(gogoproto.nullable) = false, (gogoproto.casttype) = "DescriptorVersion"];
  optional util.hlc.Timestamp modification_time = 19 [(gogoproto.nullable) = false];

  // DeclarativeSchemaChangerState contains the state corresponding to the
  // descriptor being changed as part of a declarative schema change.
  optional cockroach.sql.schemachanger.scpb.DescriptorState declarative_schema_changer_state = 20;
  // Next field id is 21
}
```

## Function OIDs

Currently we reference functions by names in all column/index/constraint
expressions used within a table. Namely, we store expressions as a string in
which function calls are represented in the format of `func_name(params..)`.
This is good enough for builtin functions which are the only functions we
currently have, but it would be problematic for UDFs since they can be renamed.
It’s necessary that function references are tracked by non-name identifiers in
expressions.

To address that, OIDs will be assigned to UDF as `Descriptor ID + 100k`, this is
the same way as we assign OIDs to user defined enum types nowadays. For
builtins, currently OID (used for ::REGPROC casting and `pg_proc` table) is
assigned at init time by hashing on builtin function signatures, this is
unfortunately a obstacle in our way because the hash OID might conflict with any
UDF descriptor OIDs. To clean the way, fixture OIDs will be assigned to builtin
functions. Similar to OIDs of builtin data types,  builtin function OIDs will
start from `1` and less than `100k` (which should be enough). This won’t break
anything of CRDB internally, but hopefully no customer depends on the current
OIDs generated from hashing.

However, one downside of this approach is that we need to manually give each
builtin an OID which is quite tedious. Instead we could utilize the builtin
registration at init time to auto assign OIDs with a counter. One problem came
up is that we might move the builtin definitions around between files and OIDs
could change because it's hard to tell which definition goes first at init time.
So referencing builtin functions by OIDs in expressions would fail. One
compromise we could make is that we reference builtin functions by name in
expression, but reference UDF functions by OIDs. This seems inconsistent, but
would save a lot of pain.

To make OID function reference parsing easier in a SQL expression, we propose
a new SQL token `OID_FUNC_REF` in the SQL parser. Which essentially represents
a string in the format of `@Fxxx` where `xxx` is an OID integer. Choosing this
syntax instead of using existing syntax for Table (e.g. `oid(1, 2)`) and type
(e.g. `@oid`) mainly because we want to avoid any possible ambiguities for when
parsing statement or expressions. It makes it easy to know what it represents
by simply looking at the syntax. It also allows opportunities for the existing
syntax to expand for new use cases as well. Choosing `@` because it's an obvious
symbol character which is not an operator used by any expression. Following by
an `F` make it easy to be interpreted as a `Function`. This also analogous to
type's syntax. With that, the current SQL token scanner needs to be extended
to recognize the two character string `@F` and scan an oid number after, then
set token id to `OID_FUNC_REF`. We also need a new implementation of the
`tree.FunctionReference` interface, which essentially is just an OID,  to
represent resolvable function OIDs. This would be the value type of the new
`OID_FUNC_REF` token.

## Function Body Validation

UDF bodies should be validated at definition time. All SQL statements in the
function body should be valid SQL strings that can be parsed and executed sanely
under the current database and schema search path. This lays out the requirement
that we need to at least be able to build a `memo` (not necessarily optimized)
for each of the statements, so that we’re sure that all referenced objects
(table, function and etc.) and subobjects (column, index and etc.) are valid.
The optimizer should also be educated about the argument names so that it can
distinguish arguments from table columns in different scope of SQL statements
(see the [Optimization and Execution](#optimization-and-execution) section for
details). Lastly, the result of the last statement must match the return type of
the function.

We will use the optimizer and the resulting memo to perform this validation.
`optbuilder` already performs many validations for SQL statements, including
object resolution, and we can extract the result type of the last statement from
the `memo`.

We aim to validate that the user-given volatility matches the actual volatility
of the statements of the function body. Postgres does not perform this
validation (see [Appendix B](#appendix-b)), allowing users to do things like
create a computed column with a UDF that is marked as immutable but is actually
volatile. This is dangerous because it can corrupt indexes and cause incorrect
query results. We propose differing from Postgres's behavior to provide
additional safety for our users.

## UDF Descriptor Backup and Restore

CRDB currently allows users to back up and restore a full cluster, an individual
database, or an individual table. We need to make sure UDF descriptors are
properly handled during backup and restore similar to current handling of views
and sequences. A UDF has similarities to both views and sequences:

1. Similar to a view, a UDF contains queries in the function body which need to be
   rewritten to reflect new db names (target database being restored into).
2. Similar to a sequence, a UDF will be referenced by other objects by id, and
   descriptor ids are rewritten during restore.

Backup of a full cluster and a full database are quite straightforward for UDFs.
Just back up everything. However, when doing backup of an individual table,
similarly to user defined enum types, we need to also back up UDFs referenced
by the table. We won’t go into the rabbit hole of backing up all referenced
tables in function bodies and all referenced objects by those tables. This would
be at users’ risk to back up individual tables. This is true nowadays, for
example, for foreign key referenced tables as well. Restoring from such backup
fails because the foreign key table cannot be found in the backup manifest.

Restoring is more interesting because we need to carefully validate that:

1. Objects on the dependency graph exist.
2. UDF descriptor ids are properly rewritten.
3. Queries in UDF function bodies are properly rewritten with target
   database name.
4. UDF OIDs referenced by tables/views are properly rewritten.

## Optimization and Execution

Several components within the optimizer will require changes in order to plan
queries with UDFs. `optbuilder` must convert a UDF reference into an optimizer
expression, `execbuilder` must convert the optimizer expression into an
evaluate-able `tree.TypedExpr` or `exec.Plan`, and normalization rules will
inline UDF function bodies directly into optimizer expressions when possible.

When `optbuilder` encounters a function reference that resolves to a UDF, it
will construct a new optimizer expression called a "routine" (this name is used
to differentiate a UDF from the existing "function" expression and matches the
nomenclature used in PG's [`DROP
ROUTINE`](https://www.postgresql.org/docs/11/sql-droproutine.html) which drops a
function or a stored procedure). The routine expression will contain a list of
relational expressions representing each statement in the UDF body. It will also
contain a list of scalar expressions representing the arguments to the UDF, and
metadata like the UDF's name, argument names, volatility, and return type.

Argument references within a UDF body will be built into the memo as "routine
variables" which track the ordinal of the argument they reference. We
deliberately do not use existing placeholder expressions used for prepared
statements to avoid incorrectly replacing a routine variable with a prepared
statement value. For example, in a prepared statement that calls a UDF, there
may be two separate `$1` variables that need to be replaced with different
values; one for the prepared statement argument and one for the UDF argument.

Names of columns in the given SQL statement take precedence over UDF arguments
of the same name. Therefore, when resolving a variable reference, the current
scope is searched before searching the names of arguments in a UDF. A UDF
argument can be qualified with the function name to avoid this:
`function_name.argument_name`. If this conflicts with a qualified column name,
the column name takes precedence.

It may be required to create two types of routine expressions: scalar routines
which return a single value, and relational routines which return multiple rows.
This detail will be ironed out during implementation.

After optimization is complete, `execbuilder` will transform a scalar routine
into a `tree.TypedExpr` that can be evaluated during execution. This typed
expression will contain a planning closure that returns an optimized `exec.Plan`
for each statement in the UDF when given the argument values. Similarly, a
relational routine will be converted into a `execPlan` that contains a planning
closure for planning each statement in the UDF based on argument values.

When a UDF is evaluated, the planning closure will be invoked for each statement
in the UDF. The closure will replace argument references with constant values
and then optimize the relational expression before constructing an `exec.Plan`.
The `exec.Plan` for each statement will be executed sequentially, and the
results of the last statement will be returned.

This approach, which is similar to the pattern used for the execution of
apply-joins, will preclude the distribution of queries with UDFs. Any query plan
with a UDF cannot be distributed because the planning closure exists in memory
on the gateway node. It may be possible in the future to partially distribute
query plans with UDFs (see [Distributing Plans with
UDFs](#distributing-plans-with-udfs)). Note that inlined UDFs (see
[Inlining](#inlining)) can be distributed as long as the operators they are
transformed into are distributable.

### Volatility

The volatility of a function has an important effect on the visibility of
mutations made by the statement calling the UDF. These mutations will be visible
to `VOLATILE` functions because it effectively starts a new snapshot for every
statement executed within it. `STABLE` and `IMMUTABLE` functions do not see
mutations made by their calling statement. They see a snapshot of the data as of
the start of the query. See [Appendix A](#appendix-a) for examples.

The `sql.planner` struct will need to configure transaction stepping for
`VOLATILE` functions, and reset the read sequence number to the original
transaction sequence number once evaluation of the UDF is complete.

### Inlining

Some UDF bodies can be inlined as relational expressions directly into the query
plan. This will improve the performance of executing a UDF because it will avoid
the overhead of the default method of execution mentioned above. Additionally,
it will allow for better row count and cost estimates, which will lead to more
efficient query plans in some cases. Because inlining should always yield a more
performant query, it can be implemented as a normalization rule.

The initial requirements for inlining a UDF are:

1. The UDF body contains a single statement.
2. The UDF performs no writes.
3. The UDF body is immutable or stable (it may be possible to relax this in some
   cases in the future).

Postgres has [additional requirements for
inlining](https://github.com/postgres/postgres/blob/db3a660c6327a6df81a55c4aa86e6c0837ecd505/src/backend/optimizer/util/clauses.c#L4507-L4535)
which we may need to heed.

#### Scalar Inlining

A scalar UDF returning a single value can be inlined directly into a projection
or filter condition. For example, in the query below, `add(a, b)` can be inlined
directly into the filter:

```sql
CREATE TABLE t (
  k INT PRIMARY KEY,
  a INT,
  b INT
);

CREATE FUNCTION add(x INT, y INT) RETURNS INT LANGUAGE SQL AS $$
  SELECT x + y;
$$;

SELECT k FROM t WHERE add(a, b) = 100;
```

The query plan would be simplified to:

```
project
 ├── columns: k:1
 └── select
      ├── columns: k:1 a:2 b:3
      ├── scan t
      │   └── columns: k:1 a:2 b:3
      └── filters
           └── (a:2 + b:3) = 100
```

More complex scalar UDFs that read from tables can also be inlined. For example,
in the query below, `max_s_k(a)` can be inlined as a correlated subquery:

```sql
CREATE TABLE t (k INT PRIMARY KEY, a INT);
CREATE TABLE s (k INT PRIMARY KEY, a INT);

CREATE FUNCTION max_s_k(t_a INT) RETURNS INT LANGUAGE SQL AS $$
  SELECT max(k) FROM s WHERE s.a = t_a;
$$;

SELECT * FROM t WHERE k = max_s_k(a);
```

After inlining, the query plan would look similar to the plan below. Existing
optimization rules would further transform the subquery into a join.

```
project
 ├── columns: k:1!null a:2
 └── select
      ├── columns: t.k:1!null t.a:2
      ├── scan t
      │    └── columns: t.k:1!null t.a:2
      └── filters
           └── eq [outer=(1,2), correlated-subquery]
                ├── t.k:1
                └── subquery
                     └── limit
                          ├── columns: max:9
                          ├── scalar-group-by
                          │    ├── columns: max:9
                          │    ├── project
                          │    │    ├── columns: s.k:5!null
                          │    │    └── select
                          │    │         ├── columns: s.k:5!null s.a:6!null
                          │    │         ├── outer: (2)
                          │    │         ├── scan s
                          │    │         │    └─── columns: s.k:5!null s.a:6
                          │    │         └── filters
                          │    │              └── s.a:6 = t.a:2
                          │    └── aggregations
                          │         └── max [as=max:9, outer=(5)]
                          │              └── s.k:5
                          └── 1
```

#### Relational Inlining

Single-statement UDFs that return multiple rows can also be inlined. Consider
the example:

```sql
CREATE TABLE scores (
  id INT PRIMARY KEY,
  user_id INT,
  score INT,
  created_at TIMESTAMPTZ
);

CREATE FUNCTION recent_scores() RETURNS SETOF INT LANGUAGE SQL AS $$
  SELECT score FROM scores WHERE created_at >= now() - '1 hour'::INTERVAL;
$$;

SELECT s AS top_recent_score FROM recent_scores() AS r(s) ORDER BY s LIMIT 10;
```

`recent_scores()` can be inlined directly into the query plan:

```
limit
 ├── columns: top_recent_score:3
 ├── ordering: +3
 ├── sort
 │    ├── columns: score:3
 │    ├── limit hint: 10.00
 │    └── project
 │         ├── columns: score:3
 │         └── select
 │              ├── columns: id:1!null user_id:2 score:3 created_at:4!null
 │              ├── scan scores
 │              │    └── columns: id:1!null user_id:2 score:3 created_at:4
 │              └── filters
 │                   └── created_at:4 >= (now() - '01:00:00')
 └── 10
```

### Statistics and Costing

If a routine is inlined, no changes to the optimizer will be required to
accurately estimate their row counts and costs. They will be transformed into
existing relational and scalar expressions, and the statistic builder and coster
won't even be aware that they originated from a UDF.

When a routine is not inlined, it will be difficult to accurately estimate their
cost. With non-constant arguments, the routine's list of relational expressions
may not be fully optimized and the number of rows returned by the routine may
not be known. Therefore, we will assume that a set returning routine returns
1000 rows and the cost of a routine is 1000 times [the sequential IO cost
factor](https://github.com/cockroachdb/cockroach/blob/7c5d0e4155006fb520942568d21a63661f3a6a0e/pkg/sql/opt/xform/coster.go#L106).
In the future, the `COST` and `ROWS` options will allow users to hint the row
count and cost to the optimizer so that it can better optimize queries that call
UDFs.

## Future Work

### Stored Procedures

Postgres's [stored
procedures](https://www.postgresql.org/docs/current/xproc.html) are similar to
user-defined functions, but can only be invoked in a `CALL <procedure>`
statement. This more restrictive nature of store procedures should allow them to
be implemented fairly easily once UDFs are supported.

### Function Body Rewrite

With the current design, any objects referenced in the function body cannot be
renamed. UDF would be broken because the renamed object cannot be resolved with
the old name and we simply disallow name changes to referenced objects. This can
be fixed if we can rewrite the function body so that all references are
represented by OIDs in queries. Or we can store the function body in the format
of an AST in which all table/view/function and even column/index are all
referenced by OID, so that function body queries can be regenerated dynamically.

Note that the same problem exists with views, the work to fix it would overlap
heavily to support renaming objects referenced in UDFs. This will be one of the
first few things we need to work on after the initial phase is complete.

### Descriptor atomic visibility

Cross-referenced objects changed within one transaction should be seen
atomically. For example, say, we have existing UDF and table:

```sql
CREATE FUNCTION toy_func() RETURNS INT LANGUAGE SQL $$ SELECT 1 $$;
CREATE TABLE t (a INT PRIMARY KEY, b INT DEFAULT (toy_func(b) + 1));
```

We modify them in a transaction:

```sql
BEGIN;
CREATE OR REPLACE FUNCTION toy_func() RETURNS INT LANGUAGE SQL $$ SELECT -1 $$;
ALTER TABLE t ALTER COLUMN b SET DEFAULT (toy_func(b) - 1);
COMMIT;
```

Any inserts into table `t` should lead to default values of either 2 or -2 for
column `b`. Any other results caused by interleaved versions of the two objects
should be considered invalid. This is a quite minor problem we currently have in
our leasing system that needs to be fixed for correctness.

### Recursion and Function Cycles

Supporting recursive functions and cycles between functions (e.g., `fn1` calls
`fn2` and `fn2` calls `fn1`) will require changes to query planning and
execution. In particular, we'll need a way to reference a routine expression in
the query plan to avoid building an expression tree with infinite depth.

### Procedural languages

We'll likely want to add support for
[PL/pgSQL](https://www.postgresql.org/docs/current/plpgsql.html) UDFs in the
near future because they are commonly used. We'd need to add execution support
for `VARIABLE`s, `IF`s, `LOOP`s, and `RETURN`s. The [Froid
paper](https://www.vldb.org/pvldb/vol11/p432-ramachandra.pdf) is a good starting
point for optimizing these constructs into relational expressions, though these
optimizations aren't required to support PL/pgSQL (Postgres does not perform
these optimizations).

Supporting other procedural languages, like C, Python, or Perl, will be more
difficult. Early experiments of supporting procedural languages with a
[WebAssembly runtime](https://github.com/wasmerio/wasmer-go) have shown promise.

### DDL (Schema Changes) in UDFs

As far as we know, nothing in this proposal prevents us from supporting schema
changes in UDFs. Performing query planning of the statements within a UDF during
the planning stage of the calling statement is valid because statements within a
UDF see a snapshot of the catalog from the start of the transaction, even if
they are `VOLATILE` (see [Appendix C](#appendix-c)). That being said, the
usefulness of UDFs with schema changes in a production application is unclear,
and supporting them would require significant testing to ensure correct behavior
with all types of schema changes.

### UDFs in change feed expression

This won't be supported at least in the initial version but can be explored to
support more complex filtering.

### Distributing Plans with UDFs

In theory it is possible to distribute parts of a query plan even if a UDF
`planNode` must be executed on the gateway node. DistSQL does not currently support
this type of partial distribution. Lifting this restriction would benefit the
execution of apply-joins as well by allowing parts of a query plan with an
apply-join to be distributed.

## Appendix

### Appendix A

This example shows how mutations to data made by a statement calling a UDF are
seen by a `VOLATILE` UDF, but not by a `STABLE` UDF. Notice how `incr_t_a()` and
`get_t_a_volatile()` return the incrementing value while `get_t_a_stable()` sees
the value as of the start of the query.

```
postgres=# CREATE TABLE t (k INT PRIMARY KEY, a INT);
CREATE TABLE

postgres=# INSERT INTO t VALUES (1, 0);
INSERT 0 1

postgres=# CREATE FUNCTION incr_t_a() RETURNS INT LANGUAGE SQL AS $$
postgres$#   UPDATE t SET a = a + 1 WHERE k = 1 RETURNING a;
postgres$# $$;
CREATE FUNCTION

postgres=# CREATE FUNCTION get_t_a_stable() RETURNS INT STABLE LANGUAGE SQL AS $$
postgres$#   SELECT a FROM t WHERE k = 1;
postgres$# $$;
CREATE FUNCTION

postgres=# CREATE FUNCTION get_t_a_volatile() RETURNS INT VOLATILE LANGUAGE SQL AS $$
postgres$#   SELECT a FROM t WHERE k = 1;
$$;
postgres$# CREATE FUNCTION

postgres=# SELECT i, incr_t_a(), get_t_a_stable(), get_t_a_volatile() FROM generate_series(1, 10) g(i);
 i  | incr_t_a | get_t_a_stable | get_t_a_volatile
----+----------+----------------+------------------
  1 |        1 |              0 |                1
  2 |        2 |              0 |                2
  3 |        3 |              0 |                3
  4 |        4 |              0 |                4
  5 |        5 |              0 |                5
  6 |        6 |              0 |                6
  7 |        7 |              0 |                7
  8 |        8 |              0 |                8
  9 |        9 |              0 |                9
 10 |       10 |              0 |               10
(10 rows)
```

### Appendix B

This example shows how Postgres does not verify that the given volatility of a
UDF matches the volatility of the statements in the UDF body. This allows users
to do things like create a computed column with a UDF that is marked as
immutable but is actually volatile, which is dangerous because it can corrupt
indexes and cause incorrect query results.

```
postgres=# CREATE TABLE t (a INT GENERATED ALWAYS AS (rand()) STORED);
ERROR:  42P17: generation expression is not immutable
LOCATION:  cookDefault, heap.c:3103

postgres=# CREATE FUNCTION immutable_rand() RETURNS IMMUTABLE INT LANGUAGE SQL AS $$
postgres$#   SELECT rand();
postgres$# $$;
CREATE FUNCTION

postgres=# CREATE TABLE t (a INT GENERATED ALWAYS AS (immutable_rand()) STORED);
CREATE TABLE
```

### Appendix C

These examples show how UDFs in Postgres see a snapshot of the catalog from the
start of the transaction, even if they are volatile. This first example shows
how Postgres doesn't allow creating a UDF that creates a table and inserts into
that table.

```
postgres=# CREATE FUNCTION create_and_insert() RETURNS BOOL VOLATILE LANGUAGE SQL AS $$
postgres$#   CREATE TABLE t (k INT PRIMARY KEY, a INT);
postgres$#   INSERT INTO t VALUES (1, 10);
postgres$# $$;
ERROR:  42P01: relation "t" does not exist
LINE 3:   INSERT INTO t VALUES (1, 10);
                      ^
LOCATION:  parserOpenTable, parse_relation.c:1384
```

We can get around this by creating a table, then creating the function, and
finally dropping the table. When the function is called, we see that the insert
fails because the newly created table is not visible.

```
postgres=# CREATE TABLE t (k INT PRIMARY KEY, a INT);
CREATE TABLE

postgres=# CREATE FUNCTION create_and_insert() RETURNS BOOL VOLATILE LANGUAGE SQL AS $$
postgres$#   CREATE TABLE t (k INT PRIMARY KEY, a INT);
postgres$#   INSERT INTO t VALUES (1, 10);
postgres$#   SELECT true;
postgres$# $$;
CREATE FUNCTION

postgres=# DROP TABLE t;
DROP TABLE

postgres=# SELECT create_and_insert();
ERROR:  42P01: relation "t" does not exist
LINE 3:   INSERT INTO t VALUES (1, 10);
                      ^
QUERY:
  CREATE TABLE t (k INT PRIMARY KEY, a INT);
  INSERT INTO t VALUES (1, 10);
  SELECT true;

CONTEXT:  SQL function "create_and_insert" during startup
LOCATION:  parserOpenTable, parse_relation.c:1384
```

### Appendix D

This example shows how UDFs in Postgres, when defined with string function
bodies where references are resolved at execution time, can cause index
corruption. Notice how the last two `SELECT` statements return different results
because one performs an index-only scan of the corrupt secondary index, while
the other performs a full table scan.

```
postgres=# CREATE FUNCTION f1() RETURNS INT IMMUTABLE LANGUAGE SQL AS 'SELECT 1';
CREATE FUNCTION

postgres=# CREATE FUNCTION f2() RETURNS INT IMMUTABLE LANGUAGE SQL AS 'SELECT f1()';
CREATE FUNCTION

postgres=# CREATE TABLE t (k INT PRIMARY KEY, a INT);
CREATE TABLE

postgres=# CREATE INDEX idx ON t ((a + f2())) INCLUDE (k, a);
CREATE INDEX

postgres=# INSERT INTO t(k, a) SELECT i, i FROM generate_series(2, 100000) g(i);
INSERT 0 99999

postgres=# ANALYZE t;
ANALYZE

postgres=# CREATE OR REPLACE FUNCTION f1() RETURNS INT IMMUTABLE LANGUAGE SQL AS 'SELECT 2';
CREATE FUNCTION

postgres=# INSERT INTO t(k, a) VALUES (1, 1);
INSERT 0 1

postgres=# CREATE OR REPLACE FUNCTION f1() RETURNS INT IMMUTABLE LANGUAGE SQL AS 'SELECT 1';
CREATE FUNCTION

postgres=# ANALYZE t;
ANALYZE

postgres=# EXPLAIN SELECT * FROM t WHERE a + f2() = 2;
                            QUERY PLAN
------------------------------------------------------------------
 Index Only Scan using idx on t  (cost=0.29..8.31 rows=1 width=8)
   Index Cond: (((a + 1)) = 2)
(2 rows)

postgres=# SELECT * FROM t WHERE a + f2() = 2;
 k | a
---+---
(0 rows)

postgres=# DROP INDEX idx;
DROP INDEX

postgres=# SELECT * FROM t WHERE a + f2() = 2;
 k | a
---+---
 1 | 1
(1 row)
```
