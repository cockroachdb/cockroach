# CockroachDB Style guide

## Go Code
We're following the
[Google Go Code Review](https://code.google.com/p/go-wiki/wiki/CodeReviewComments)
fairly closely. In particular, you want to watch out for proper
punctuation and capitalization in comments. We use two-space indents
in non-Go code (in Go, we follow `gofmt` which indents with
tabs).

### Line Length
Format your code assuming it will be read in a window 100 columns wide.
Wrap code at 100 characters and comments at 80 unless doing so makes the
code less legible. These values assume tab width is 2 characters.

### Wrapping Function Signatures
When wrapping function signatures that do not fit on one line,
put the name, arguments, and return types on separate lines, with the closing `)`
at the same indentation as `func` (this helps visually separate the indented
arguments from the indented function body). Example:
```go
func (s *someType) myFunctionName(
    arg1 somepackage.SomeArgType, arg2 int, arg3 somepackage.SomeOtherType,
) (somepackage.SomeReturnType, error) {
    ...
}
```

If the arguments list is too long to fit on a single line, switch to one
argument per line:
```go
func (s *someType) myFunctionName(
    arg1 somepackage.SomeArgType,
    arg2 int,
    arg3 somepackage.SomeOtherType,
) (somepackage.SomeReturnType, error) {
    ...
}
```

If the return types need to be wrapped, use the same rules:
```go
func (s *someType) myFunctionName(
    arg1 somepackage.SomeArgType, arg2 somepackage.SomeOtherType,
) (
    somepackage.SomeReturnType,
    somepackage.SomeOtherType,
    error,
) {
    ...
}
```

Exception when omitting repeated types for consecutive arguments:
short and related arguments (e.g. `start, end int64`) should either go on the same line
or the type should be repeated on each line -- no argument should appear by itself
on a line with no type (confusing and brittle when edited).

### Wrapping Function Calls

Note: the guidelines in this section are suggestions; they are not required and
they should not be demanded in code reviews.

When wrapping function calls that do not fit on one line, put the arguments on a
separate line, with the closing `)` on a separate line:

```go
    someFunc(
       arg1, arg2, arg3,
    )
```

If this still doesn't fit, put each argument on a separate line:

```go
    someFunc(
       arg1,
       arg2,
       arg3,
    )
```

This form should also be used if one of the arguments is a multi-line expression:

```go
   someFunc(
     arg1,
     arg2{
       field1: val1,
       field2: val2,
     },
   )
```

A complex example:
```go
  if err := txn.Exec(
    client.TxnExecOptions{AutoRetry: false, AutoCommit: true},
    func(txn *client.Txn, _ *client.TxnExecOptions) error {
      // Set deadline to sometime in the past.
      txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: timeutil.Now().Add(-time.Second).UnixNano()})
      _, err := txn.Get("k")
      return err
    },
  ); !testutils.IsError(err, "txn aborted") {
    t.Fatal(err)
  }
```
                          }
### fmt Verbs

Prefer the most specific verb for your use. In other words, prefer to avoid `%v`
when possible. However, `%v` is to be used when formatting bindings which might
be `nil` and which do not already handle `nil` formatting. Notably, `nil` errors
formatted as `%s` will render as `%!s(<nil>)` while `nil` errors formatted as `%v`
will render as `<nil>`. Therefore, prefer `%v` when formatting errors which are
not known to be non-`nil`.
