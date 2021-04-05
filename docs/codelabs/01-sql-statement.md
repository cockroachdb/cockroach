# Codelab: Adding a SQL Statement

## Background

This codelab will walk you through adding a new SQL statement to the parser.

## Getting Started

Before we get started, you need to download the CockroachDB source code and
ensure you have all of the prerequisites needed for development. See
[CONTRIBUTING.md] doc for details.

It might also be useful to first walk through the codelab [Adding a SQL Function][sql-function],
which provides a more gentle introduction to the SQL type system as well as a
good coverage of how to add tests, which this codelab ignores.

Also, remember that for real feature development, you'll want to first write up
an RFC describing the new feature as well as the proposed syntax, and make sure
to get the approval of someone from @cockroachdb/sql-language.  There are also
some guidelines on adding new syntax that you can read about on [#17569](https://github.com/cockroachdb/cockroach/pull/17569).

## Adding a SQL Statement

CockroachDB supports many different types of [SQL statements][statements].
This Codelab describes the process of adding a novel statement type to the SQL
parser, its implementation, and the requisite tests.  We'll see how to work with
the `goyacc` tool to update the parser and see how the executor and the query
planner work together to execute queries.

### Syntax and Grammars and Keywords, oh my!

Adding a new SQL statement starts with adding the necessary syntax to the SQL
parser.  The parser is produced from a grammar file by `goyacc`, a Go flavor of
the popular `yacc` compiler compiler.  The source grammar is located at
`pkg/sql/parser/sql.y`.  The output of the parser is an abstract syntax tree,
with node types defined in various files under `pkg/sql/sem/tree`.

There are three main components to adding a new statement to the SQL parser:
adding any new keywords, adding clauses to the statement parser, and adding a
new syntax node type.

### To Frobnicate

We'll add a new statement to the Cockroach dialect of SQL: `FROBNICATE`.  This
statement will randomly change the settings on the database (something we've
all wanted to do now and then).  There will be three options: `FROBNICATE
CLUSTER`, which operates on cluster settings, `FROBNICATE SESSION`, working on
session settings, and `FROBNICATE ALL`, which handles both.

Let's start by checking to make sure all our keywords are defined.  Open
`pkg/sql/parser/sql.y` and search for "Ordinary key words".  You'll find a series of token
definitions in alphabetical order.  Since the grammar already uses `SESSION`,
`CLUSTER`, and `ALL` keywords, we don't need to add those, but we do need to
make a keyword for `FROBNICATE`.  It should look like this:

```text
%token <str> FROBNICATE
```

This tells the lexer to recognize the keyword, but we still need to add it to
one of the category lists.  If the keyword can ever appear in an identifier
position, it has to be reserved (which requires that other uses of it, for
instance as a column name, must be quoted).  Since our new keyword must start
the statement, it can't be confused for an identifier, so we can safely add it
to the unreserved keywords list.

```text
unreserved_keyword:
...
| FROBNICATE
...
```

Now that the lexical analyzer knows about all our keywords, we need to teach the
parser how to handle our new statement.  There are three places that we need to
add references: the type list, the statement cases list, and the parsing clause.

Search in the grammar file for `<tree.Statement>`, and you'll find the type list.
Add a line for our new statement type, something like:

```text
%type <tree.Statement> frobnicate_stmt
```

Now search for `stmt:` to find the list of productions for the `stmt` rule.  Add
a case for our statement type.

```text
stmt:
...
| frobnicate_stmt // EXTEND WITH HELP: FROBNICATE
...
```

Finally, we need to add a production rule for our statement.  Somewhere below the
rule for `stmt` (perhaps in alphabetical order?) add our rule.  For now we'll
leave it unimplemented, but we'll come back and take care of that later.

```text
frobnicate_stmt:
  FROBNICATE CLUSTER { return unimplemented(sqllex, "frobnicate cluster") }
| FROBNICATE SESSION { return unimplemented(sqllex, "frobnicate session") }
| FROBNICATE ALL { return unimplemented(sqllex, "frobnicate all") }
```

This lists the three forms of the expression that we'll allow, separated by the
pipe character.  Each production also has an implementation in curly braces
(though in this case the implementation is to error out with an unimplemented
message).

One last thing - let's implement the help for our statement right now.  Above the
production rule, let's add the following comments:

```text
// %Help: FROBNICATE - twiddle the various settings
// %Category: Misc
// %Text: FROBNICATE { CLUSTER | SESSION | ALL }
```

That's it!  Now our parser will recognize the new statement type, and the help
generators will provide assistance to users.  Let's give it a try.  First, we
need to regenerate the file `sql.go`:

```text
~/go/src/github.com/cockroachdb/cockroach$ make generate
...
Type checking sql.y
Compiling sql.go
...
```

And then compile the project:

```text
~/go/src/github.com/cockroachdb/cockroach$ make build
...
github.com/cockroachdb/cockroach
```

Now, let’s run a single-node Cockroach instance:

```text
$ rm -fr cockroach-data/ && ./cockroach start-single-node --insecure
...
status:     initialized new cluster
...
```

In another terminal window, use the `cockroach sql` shell to try out our new
statement:

```text
$ ./cockroach sql --insecure -e "frobnicate cluster"
ERROR: at or near "cluster": syntax error: unimplemented: this syntax
SQLSTATE: 0A000
DETAIL: source SQL:
frobnicate cluster
           ^

HINT: You have attempted to use a feature that is not yet implemented.

Please check the public issue tracker to check whether this problem is
already tracked. If you cannot find it there, please report the error
with details by creating a new issue.

If you would rather not post publicly, please contact us directly
using the support form.

We appreciate your feedback.
Failed running "sql"
```

Hooray!  Our syntax is parsing successfully and then failing to do anything.
Notice that the error specifies that the statement is unimplemented.  If we try
something invalid we'll see a different error:

```go
$ ./cockroach sql --insecure -e 'hodgepodge bananas'
ERROR: at or near "hodgepodge": syntax error
SQLSTATE: 42601
DETAIL: source SQL:
hodgepodge bananas
^
Failed running "sql"
```

### A forest of Abstract Syntax Trees

Now that we've handled the syntax, we need to give our new statement the
appropriate semantics.  We'll need an AST node to communicate the structure of
the statement from the parser to the runtime.  Remember when we said our
statement is of `%type <tree.Statement>`?  That means it needs to implement the
`tree.Statement` interface, which can be found in `pkg/sql/sem/tree/stmt.go`.
There are four functions we need to write: three for the `Statement` interface
itself (`StatementReturnType`, `StatementType` and `StatementTag`), one for
`NodeFormatter` (`Format`), and the standard `fmt.Stringer`.

Make a new file for our statement type: `pkg/sql/sem/tree/frobnicate.go`.  In
it, put the implementation of our AST node.

```go
package tree

type Frobnicate struct {
  Mode FrobnicateMode
}

var _ Statement = &Frobnicate{}

type FrobnicateMode int

const (
  FrobnicateModeAll FrobnicateMode = iota
  FrobnicateModeCluster
  FrobnicateModeSession
)

func (node *Frobnicate) StatementReturnType() StatementReturnType { return Ack }
func (node *Frobnicate) StatementTag() string               { return "FROBNICATE" }

func (node *Frobnicate) Format(ctx *FmtCtx) {
  ctx.WriteString("FROBNICATE ")
  switch node.Mode {
  case FrobnicateModeAll:
    ctx.WriteString("ALL")
  case FrobnicateModeCluster:
    ctx.WriteString("CLUSTER")
  case FrobnicateModeSession:
    ctx.WriteString("SESSION")
  }

  func(node *Frobnicate) String()
  string{
    return AsString(node)
  }
```

Now we need to update the parser to return a `Frobnicate` node with the
appropriate mode type when it encounters our syntax.  But before we do, let's
write a test for our parser changes.

### Testing the parser

The parser tests are in `pkg/sql/parser/parse_test.go`.  For the most part these
tests are simply a list of example statements that should parse correctly.  Find
the right place to stick in the frobnicate cases, and add one for each type:

```go
// ...
	{`FROBNICATE CLUSTER`},
	{`FROBNICATE SESSION`},
	{`FROBNICATE ALL`},
// ...
```

Then rebuild and run the tests to watch them fail:

```text
$ make test
...
--- FAIL: TestParse (0.00s)
    parse_test.go:721: FROBNICATE CLUSTER: expected success, but found unimplemented at or near "cluster"
        FROBNICATE CLUSTER
...
```

Great, a failing test!  Let's make it pass.

### Finishing the parser changes

```text
frobnicate_stmt:
  FROBNICATE CLUSTER { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeCluster} }
| FROBNICATE SESSION { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeSession} }
| FROBNICATE ALL { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeAll} }
```

The special symbol `$$.val` represents the node value that this rule generates.
There are a few other `$` symbols that you can use with yacc.  One of the more
useful forms refers to node values of sub-productions (for instance, in these
three statements `$1` would be the token `FROBNICATE`).

Rebuild the project (don't forget to regenerate the parser) and try the test
one more time:

```text
$ make test
```

If we did everything correctly so far, there are no more failing tests!  Now
try out the statement again:

```text
$ ./cockroach sql --insecure -e "frobnicate cluster"
Error: pq: unknown statement type: *tree.Frobnicate
Failed running "sql"
```

Progress!  We're seeing a different error now.  This one is from the SQL
planner, which doesn't know what to do when it sees the new statement
type.  We need to teach it what the new statement means.  Even though our
statement won't play a part in any query plan, we'll implement it by adding
a method to the planner.  That's where the centralized statement dispatch takes
place, so that's the place to add semantics.

Look for the source of the error we're seeing.  You'll find that it's at the end
of a long type switch statement in `/pkg/sql/opaque.go`.  Let's add a case to that:

```go
case *tree.Frobnicate:
    plan, err = p.Frobnicate(ctx, n)
```

Also add the following under the `init()` function in the same file, `/pkg/sql/opaque.go`.

```go
&tree.Frobnicate{},
```

This calls a method (yet to be written) on the planner itself.  Let's implement
that method in `pkg/sql/frobnicate.go`.

```go
package sql

import (
    "context"

    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
    "github.com/cockroachdb/errors"
)

func (p *planner) Frobnicate(ctx context.Context, stmt *tree.Frobnicate) (planNode, error) {
    return nil, AssertionFailedf("We're not quite frobnicating yet...")
}
```

Run `make build` again and give it another go:

```text
$ ./cockroach sql --insecure -e "frobnicate cluster"
Error: pq: We're not quite frobnicating yet...
Failed running "sql"
```

Well that's promising.  We can at least make our errors bubble up to the SQL
client now.  All we have to do is figure out how to make our statement work.

### Messing with settings

According to [The Jargon File][jargon-file], "frob, twiddle, and tweak sometimes
connote points along a continuum. 'Frob' connotes aimless manipulation ... if
he's just [turning a knob] because turning a knob is fun, he's frobbing it."  To
that end, whereas the `SET` statement should generally be used to tweak session
and cluster settings, `FROB` should randomize them, right?

There are two kinds of settings to consider here: cluster settings and session settings.
If the statement is for a cluster setting, we make a call to `setClusterSetting`
to update the value.  If it's a session setting, we grab the variable from the
`varGen` map and call its `Set` method.

Let's start with the session settings, since they're a bit simpler.  Look at the
implementation of the `varGen` map in `pkg/sql/vars.go`.  
Most of these settings have a `Set` method. Some of them take a parameter, but 
it's usually pretty tightly constrained.  The `application_name` setting can be 
any arbitrary string, but `distsql` needs to be one of a specific set of options.

#### Frobnicating the session

First we'll work on the latter case.  For instance, the setting for
`distsql` must be one of `"OFF"`, `"ON"`, `"AUTO", or `"ALWAYS"`.

In `pkg/sql/frobnicate.go`:

```go
var distSQLOptions = []string{"off", "on", "auto", "always"}
```

Now we need to write a method to pick a valid option.

```go
import (
    // ...
    "math/rand"
    // ...
)

func randomMode() sessiondata.DistSQLExecMode {
    i := rand.Int() % len(distSQLOptions)
    mode, _ := sessiondata.DistSQLExecModeFromString(distSQLOptions[i])
    return mode;
}
```

Ok, two more helpers.  The `application_name` setting can be an arbitrary string,
let's write a helper to make up a random name.

```go
import (
    "bytes"
    // ...
)

func randomName() string {
    length := 10 + rand.Int() % 10
    buf := bytes.NewBuffer(make([]byte, 0, length))

    for i := 0; i < length; i++ {
        ch := 'a' + rune(rand.Int() % 26)
        buf.WriteRune(ch)
    }

    return buf.String()
}
```

Now we just need to iterate through the various settings that we can frobnicate.

```go
func (p *planner) randomizeSessionSettings() {
    mutator := p.sessionDataMutator

    mutator.SetDistSQLMode(randomMode())
    mutator.SetApplicationName(randomName())
}
```

Now let's wire it up into our statement.

```go
func (p *planner) Frobnicate(ctx context.Context, stmt *tree.Frobnicate) (planNode, error) {
    switch stmt.Mode {
    case tree.FrobnicateModeSession:
        p.randomizeSessionSettings()
    default:
        return nil, errors.AssertionFailedf("Unhandled FROBNICATE mode %v!", stmt.Mode)
    }

    return newZeroNode(nil /* columns */), nil
}
```

Okay, let's give it a try:

```text
$ ./cockroach sql --insecure -e "frobnicate session; show application_name"
  application_name
--------------------
  somdizkjyjqhr
(1 row)
```

Success!  Let's just try again, for good measure.

```text
$ ./cockroach sql --insecure -e "frobnicate session; show application_name"
  application_name
--------------------
  kgqhgelkgiige
(1 row)
```

<span></span> <!-- Force space after collapsible section. -->

#### Frobnicating the cluster

Now that we've got the session settings right, maybe we'll want to implement
frobbing of the cluster settings.  The complete implementation is left as an
exercise for the reader.

### Adding an alias statement

Now that we're regularly frobbing our database, it's going to get tiring having
to type `FROBNICATE` in full every time.  Let's add an alias, so that entering
`FROB` has the same effect.

This shouldn't require changes anywhere except in the syntax file `sql.y`.  Give
it a try, and look below if you need a hint.


<details>
  <summary>View our solution</summary>
  <p>

  <!--
    This collapsible-section hack is very sensitive to whitespace.
    Be careful! See: https://stackoverflow.com/a/39920717/1122351
  -->

  ```diff
  unreserved_keyword:
  ...
+ | FROB
  | FROBNICATE
  ...

  frobnicate_stmt:
    FROBNICATE CLUSTER { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeCluster} }
  | FROBNICATE SESSION { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeSession} }
  | FROBNICATE ALL { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeAll} }
+ | FROB CLUSTER { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeCluster} }
+ | FROB SESSION { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeSession} }
+ | FROB ALL { $$.val = &tree.Frobnicate{Mode: tree.FrobnicateModeAll} }
  ```
  </p>
</details>

<span></span> <!-- Force space after collapsible section. -->

That's it!  You've seen how to add new syntax and semantics to the CockroachDB
SQL parser and execution engine.

[CONTRIBUTING.md]: https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md
[sql-function]: https://github.com/cockroachdb/cockroach/blob/master/docs/codelabs/00-sql-function.md
[statements]: https://www.cockroachlabs.com/docs/stable/sql-statements.html
[jargon-file]: http://www.catb.org/jargon/html/F/frobnicate.html
