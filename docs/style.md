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

### Inline comments for arguments to function calls

A code reader encountering a function call should be able to intuit what all
the arguments to the call represent. Whenever it wouldn't be otherwise clear
what the value used as an argument represents (for example, from the variable's
name if a variable is used or from the type name if a struct literal is used),
consider annotating it with an inline comment specifying the respective
parameter's name. Particularly, consider doing this for literals of "basic"
types (boolean, numeric, string types, whether the type is predeclared or not)
and for `nil` identifiers, as they are frequently not suggestive enough of what
they represent.

For example:

```
intentsToEvalResult(externalIntents, args, false /* alwaysReturn */)

monitor := mon.MakeMonitor(
  "in-mem temp storage",
  mon.MemoryResource,
  nil,             /* curCount */
  nil,             /* maxHist */
  1024*1024,       /* increment */
  maxSizeBytes/10, /* noteworthy */
)
```

Note: For `bool` constants, like for all literals, the comment should indicate
the name of the parameter and does not depend on the argument value.
*Do not* put a bang in the comment when commenting the `false` constant. Also,
do not adapt the comment to negate the name of the parameter. For example:

```
func endTxn(commit bool){}

OK:     endTxn(false /* commit */)
NOT OK: endTxn(false /* !commit */)
NOT OK: endTxn(false /* abort */)
// If you want to add an explanation to an argument, a suggested style is to
// include both the param name and the explanation with a dash between them:
OK:     endTxn(false /* commit - we abort as we concluded above that we can't commit */)
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

This is not always the case. However, in cases where that is a fair assessment
of the situation, consider whether the `doSomething` function should exist at
all.  
In cases where the `bool` in question, along with other arguments, acts as a
"knob" to the function consider replacing it with some type of "configuration"
struct (for examples, see [Dave Cheney's treatment of the
topic](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)).
In situations where there's a single `bool` param or the situation is less
clear-cut, consider replacing the `bool` in question with an enum. For example:

```
type EndTxnAction bool

const (
  Commit EndTxnAction = false
  Abort = true
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
