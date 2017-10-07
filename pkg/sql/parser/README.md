# Generated Documentation and Embedded Help Texts

Many parts of the `parser` package include special consideration for generation
or other production of user-facing documentation. This includes interactive help
messages, generated documentation of the set of available functions, or diagrams
of the various expressions.

Generated documentation is produced and maintained at compile time, while the
interactive, contextual help is returned at runtime.

We equip the generated parser with the ability to report contextual
help in two circumstances:

- when the user explicitly requests help with the HELPTOKEN (current syntax: standalone "`??`")
- when the user makes a grammatical mistake (e.g. `INSERT sometable INTO(x, y) ...`)

We use the `docgen` tool to produce the generated documentation files that are
then included in the broader (handwritten) published documentation.

# Help texts embedded in the grammar

The help is embedded in the grammar using special markers in
yacc comments, for example:

```
// %Help: HELPKEY - shortdescription
// %Category: SomeCat
// %Text: whatever until next %marker at start of line, or non-comment.
// %SeeAlso: whatever until next %marker at start of line, or non-comment.
// %End (optional)
```

The "HELPKEY" becomes the map key in the generated Go map.

These texts are extracted automatically by `help.awk` and converted
into a Go data structure in `help_messages.go`.

# Support in the parser

## Primary mechanism - LALR error recovery

The primary mechanism is leveraging error recovery in LALR parsers
using the special `error` token [1] [2]: when an unexpected token is
encountered, the LALR parser will pop tokens on the stack until the
prefix matches a grammar rule with the special "`error`" token (if
any). If such a rule exists, its action is used to reduce and the
erroneous tokens are discarded.

**This mechanism is used both when the user makes a mistake, and when
the user inserts the HELPTOKEN in the middle of a statement.** When
present in the middle of a statement, HELPTOKEN is considered an error
and triggers the error recovery.

We use this for contextual help by providing `error` rules that
generate a contextual help text during LALR error recovery.

For example:

```
backup_stmt:
  BACKUP targets TO string_or_placeholder opt_as_of_clause opt_incremental opt_with_options
  {
    $$.val = &Backup{Targets: $2.targetList(), To: $4.expr(), IncrementalFrom: $6.exprs(), AsOf: $5.asOfClause(), Options: $7.kvOptions()}
  }
| BACKUP error { return helpWith(sqllex, `BACKUP`) }
```

In this example, the grammar specifies that if the BACKUP keyword is
followed by some input tokens such that the first (valid) grammar rule
doesn't apply, the parser will "recover from the error" by
backtracking up until the point it only sees `BACKUP` on the stack
followed by non-parsable tokens, at which points it takes the `error`
rule and executes its action.

The action is `return helpWith(...)`. What this does is:

- halts parsing (the generated parser executes all actions
  in a big loop; a `return` interrupts this loop);
- makes the parser return with an error (the `helpWith`
  function returns non-zero);
- extends the parsing error message with a help
  text; this help text can subsequently be exploited in a client to
  display the help message in a friendly manner.

### Code generation

Since the pattern "`{ return helpWith(sqllex, ...) }`" is common, we also implement
a shorthand syntax based on comments, for example:

```
backup_stmt:
   ...
| BACKUP error // SHOW HELP: BACKUP
```

The special comment syntax "`SHOW HELP: XXXX`" is substituted by means
of an auxiliary script (`replace_help_rules.awk`) into the form
explained above.

### Secondary mechanism - explicit help token

The mechanism described above works both when the user make a
grammatical error and when they place the HELPTOKEN in the middle of a
statement, rendering it invalid.

However for contextual help this is not sufficient: what happens if
the user requests HELPTOKEN *at a position in the grammar where
everything before is a complete, valid SQL input*?

For example: `DELETE FROM foo ?`

When encountering this input, the LALR parser will see `DELETE FROM
foo` first, then *reduce* using the DELETE action because everything
up to this point is a valid DELETE statement. When the HELPTOKEN is
encountered, the statement has already been completed *and the LALR
parser doesn't 'know' any more that it was in the context of a DELETE
statement*.

If we try to place an `error`-based recovery rule at the top-level:

```
stmt:
  alter_stmt
| backup_stmt
| ...
| delete_stmt
| ...
| error { ??? }
```

This wouldn't work: the code inside the `error` action cannot
"observe" the tokens observed so far and there would be no way to know
whether the error should be about DELETE, or instead about ALTER,
BACKUP, etc.

So in order to handle HELPTOKEN after a valid statement, we must place
it in a rule where the context is still available, that is *before the
statement's grammar rule is reduced.*

Where would that be? Suppose we had a simple statement rule:

```
somesimplestmt:
  SIMPLE DO SOMETHING { $$ = reduce(...) }
| SIMPLE error { help ... }
```

We could extend with:

```
somesimplestmt:
  SIMPLE DO SOMETHING { $$ = reduce(...) }
| SIMPLE DO SOMETHING HELPTOKEN { help ... }
| SIMPLE error { help ... }
```

(the alternative also works:

```
somesimplestmt:
  SIMPLE DO SOMETHING { $$ = reduce(...) }
| SIMPLE DO SOMETHING error { help ... }
| SIMPLE error { help ... }
```
)

That is all fine and dandy, but in SQL we have statements with many alternate forms, for example:

```
alter_rename_table_stmt:
  ALTER TABLE relation_expr RENAME TO qualified_name { ... }
| ALTER TABLE IF EXISTS relation_expr RENAME TO qualified_name { ... }
| ALTER TABLE relation_expr RENAME opt_column name TO name { ... }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name { ... }
```

To add complementary handling of the help token at the end of valid statements we could, but would hate to, duplicate all the rules:

```
alter_rename_table_stmt:
  ALTER TABLE relation_expr RENAME TO qualified_name { ... }
| ALTER TABLE relation_expr RENAME TO qualified_name HELPTOKEN { help ... }
| ALTER TABLE IF EXISTS relation_expr RENAME TO qualified_name { ... }
| ALTER TABLE IF EXISTS relation_expr RENAME TO qualified_name HELPTOKEN { help ... }
| ALTER TABLE relation_expr RENAME opt_column name TO name { ... }
| ALTER TABLE relation_expr RENAME opt_column name TO name HELPTOKEN { help ... }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name { ... }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name HELPTOKEN { help ... }
```

This duplication is horrendous (not to mention hard to maintain), so
instead we should attempt to factor the help token *in a context where
it is still known that we are dealing just with that statement*.

The following works:

```
alter_rename_table_stmt:
  real_alter_rename_table_stmt { $$ = $1 }
| real_alter_rename_table_stmt HELPTOKEN { help ... }

real_alter_rename_table_stmt:
  ALTER TABLE relation_expr RENAME TO qualified_name { ... }
| ALTER TABLE IF EXISTS relation_expr RENAME TO qualified_name { ... }
| ALTER TABLE relation_expr RENAME opt_column name TO name { ... }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name { ... }
```

Or does it? Without anything else, yacc complains with a "shift/reduce
conflict". The reason is coming from the ambiguity: when the parsing
stack contains everything sufficient to match a
`real_alter_rename_table_stmt`, there is a choice between *reducing*
the simple form `alter_rename_table_stmt:
real_alter_rename_table_stmt`, or *shifting* into the more complex
form `alter_rename_table_stmt: real_alter_rename_table_stmt
HELPTOKEN`.

This is another form of the textbook situation when yacc is used to
parse if-else statements in a programming language: the rule `stmt: IF
cond THEN body | IF cond THEN body ELSE body` is ambiguous (and yields
a shift/reduce conflict) for exactly the same reason.

The solution here is also straight out of a textbook: one simply
informs yacc of the relative priority between the two candidate
rules. In this case, when faced with a neutral choice, we encourage
yacc to shift. The particular mechanism is to tell yacc that one rule
has a *higher priority* than another.

It just so happens however that the yacc language only allows us to
set relative priorites of *tokens*, not rules. And here we have a
problem, of the two rules that need to be prioritized, only one has a
token to work with (the one with HELPTOKEN). Which token should we
prioritze for the other?

Conveniently yacc knows about this trouble and offers us an awkward,
but working solution: we can tell it "use for this rule the same
priority level as an existing token, even though the token is not part
of the rule". The syntax for this is `rule %prec TOKEN`.

We can then use this as follows:

```
alter_rename_table_stmt:
  real_alter_rename_table_stmt           %prec LOWTOKEN { $$ = $1 }
| real_alter_rename_table_stmt HELPTOKEN %prec HIGHTOKEN { help ... }
```

We could create two new pseudo-tokens for this (called `LOWTOKEN` and
`HIGHTOKEN`) however conveniently we can also reuse otherwise valid
tokens that have known relative priorities. We settled in our case on
`VALUES` (low priority) and `UMINUS` (high priority).

### Code generation

With the latter mechanism presented above the pattern

```
rule:
  somerule           %prec VALUES
| somerule HELPTOKEN %prec UMINUS { help ...}`
```

becomes super common, so we automate it with the following special syntax:

```
rule:
  somerule // EXTEND WITH HELP: XXX
```

And the code replacement in `replace_help_rules.awk` expands this to
the form above automatically.

# Generated Documentation

Documentation of the SQL functions and operators is generated by the `docgen`
utility, using `make generate PKG=./docs/...`. The markdown-formatted files are
kept in `docs/generated/sql` and should be re-generated whenever the
functions/operators they document change, and indeed if regenerating produces a
diff, a CI failure is expected.

# References

1. https://www.gnu.org/software/bison/manual/html_node/Error-Recovery.html
2. http://stackoverflow.com/questions/9796608/error-handling-in-yacc
