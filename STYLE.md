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

### Inline comments for literals

Literals of "basic" types (predeclared boolean, numeric, string types) used as
function arguments have to be accompanied by an inline comment containing the
name of the parameter, to aid code readability. Similarly, the `nil` identifier
needs to be commented when used in this way.
For example:

```
x := intentsToEvalResult(externalIntents, args, false /* alwaysReturn */)
```

Literals used as return values for functions with multiple named results must be
accompanied by an inline comment containing the name of the result. For example:

```
func countVals() (numVals int, numIntents int) {
  return 5 /* numVals */, 1 /* numIntents */
}
```

Note: For `bool` constants, like for all literals, the comment indicates the
name of the argument / result and does not depend on the argument value. *Do
not* put a bang in the comment when commenting the `false` constant. Also, do
not adapt the comment to negate the name of the parameter. For example:
```
func endTxn(commit bool){}

OK:     endTxn(false /* commit */)
NOT OK: endTxn(false /* !commit */)
NOT OK: endTxn(false /* abort */)
```

Exceptions:
1. `nil` errors shouldn't be commented when they are the last return value.
2. `nil`s and other zero vals and sentinels shouldn't be commented in return
   values when the last return value is a non-nil error.
3. Single return values (for functions with a single named result) shouldn't be
   commented.


Although  not required, consider using inline comments with names similar in
spirit to the rules above whenever they help readability. In particular:
1. Comment the blank identifier (i.e. `_`), when used in assignments from function results. For example:

   ```
   numVals, _ /* numIntents */ := countVals()
   ```

2. Comment other identifiers when their name (or type name, when they don't have
   a basic type) is not sufficient. For example:

    ```
    foo(i /* myParam1 */, Bar{} /* myParam2 */)
    ```

The inline comments for literals represent a basic requirement. The presence of
these comments should not preclude adding more explanation to the code when more
explanation is needed. Such commentary can be placed inline, or in any other
way that makes sense at a particular site. When placed inline, the format for
the explanation is `<literal> /* <param name> - explanation */`. For example:
```
endTxn(false /* commit - this transaction has no chance of committing; see above */)
```

#### Try to avoid `bool` parameters

`bool` arguments to functions are often dubious things, as they hint to code that
essentially reads like:
```
func doSomething(shouldDoX bool) {
  if shouldDoX {
    doX()
  } else {
    doY()
  }
}
```

This is not always the case; e.g. `bool` arguments to constructors are fine.
However, in cases where that is a fair assessment of the situation, consider
whether the `doSomething` function should exist at all. In situations where the
situation is less clear-cut, consider replacing the `bool` in question with an
enum. For example:
```
type EndTxnAction int

const (
  Commit EndTxnAction = iota
  Abort
)

func endTxn(action EndTxnAction) {}
```
is better than
```
func endTxn(commit bool) {}
```

### fmt Verbs

Prefer the most specific verb for your use. In other words, prefer to avoid %v
when possible. However, %v is to be used when formatting bindings which might
be nil and which do not already handle nil formatting. Notably, nil errors
formatted as %s will render as "%!s(<nil>)" while nil errors formatted as %v
will render as "<nil>". Therefore, prefer %v when formatting errors which are
not known to be non-nil.

### Distinguishing user errors from internal errors

When creating an error for something that the user did wrong (and thus isn't
indicative of an unexpected situation in our code), use `fmt.Errorf()` to create
the error.

When creating an error for an unexpected situation, use methods from the
[`errors` package that we use](https://github.com/pkg/errors), such as
`errors.New()`, `errors.Errorf()`, `errors.Wrap()`, or `errors.Wrapf()`.

The reason for this distinction is somewhat historical (#7424), but maintaining
it will help us immensely if we ever switch to using new error types for the
different situations.
