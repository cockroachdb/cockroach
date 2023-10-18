# Return Linter

> It would be nice if only `pgerror.Error` were actually returned from
> an API, even though the functions are declared as returning `error`.

```
Output of retlint --help goes here.
```

## Motivation

Due to the lack of return-type variance in Go's type system, it is
often necessary to declare a return type that is "wider" than the
actual types that will be returned.  A very common example, and the
one in which the rest of the document is framed, is that we want to
ensure that a concrete, user-facing error type, `pgerror.Error` is
actually returned from functions that declare `error` as a return type.

## Approach

Thanks to the availability of the
[`analysis`](https://godoc.org/golang.org/x/tools/go/analysis) and
[`ssa`](https://godoc.org/golang.org/x/tools/go/ssa) packages, it is
relatively straightforward to build a linter which can examine the call
graph and type flow within a package or to perform a whole-world
analysis on a program.

The input to the linter is as follows:
* A target interface, e.g. `error`.
* A set of concrete types which implement the target interface,
  e.g. `pgerror.Error`.
* A filter which selects "seed" functions that must be provable to only
  return the desired concrete types wherever the target interface is
  declared to be returned.

The linter will then perform an inductive, heuristic analysis, starting
from the seed functions. The goal of this analysis is to classify each
reachable function into those that always return one of the desired
types in place of the target interface and those that do not.  In cases
where a seed function cannot be proven to return only the desired
concrete types, a lint error will be generated.  The lint error will
include the seed function and at least one call-path chain to show the
source of the undesirable type.

## Implementation

The implementation can be structured as a tri-state, inductive,
call-graph analysis.  All functions start in an `unknown` state.

Functions are `clean` if all of the following properties apply to every
[`Value`](https://godoc.org/golang.org/x/tools/go/ssa#Value) that
appears in a
[`Return`](https://godoc.org/golang.org/x/tools/go/ssa#Return)
instruction:
* The value is not assignable to the target interface.
* The value is, trivially, one of the acceptable concrete types or a
  pointer thereto.
  * `return &pgerror.Error{}`
  * `err := &pgerror.Error{}; return err`
  * `if pg, ok := err.(*pgerror.Error); ok { return pg }`
* The value has been type-asserted in all dominating
  instruction or basic blocks.
  * `pgErr := err.(*pgerror.Error); return err`
  * `if _, ok := err.(*pgerror.Error); ok { return err }`
* The value is passed-through from another `clean` function.
  * `if _, err := knownCleanFunction(); err != nil { return err }`

In the case of cyclical call-graphs, we will assume that functions
reachable from themselves are initially `clean` and create an
invalidation set to record the members of the cycle.  If any member of
the set is marked as `dirty`, all functions in the set will be dirtied.

Whenever a [`Phi`](https://godoc.org/golang.org/x/tools/go/ssa#Phi)
value is encountered, it will be considered `clean` only if all of its
edges are `clean`.

## Known limitations

Functions are unnecessarily `dirty` if they:
* Return a value from an indirect invocation.
  * `if _, err := someCallback(); err != nil { return err }`
* Return a value from an interface invocation.
  * `if _, err := someIntf.doSomething(); err != nil { return err }`
  * This could be lifted in a whole-program analysis by examining
    all `doSomething()` functions defined on declared types which
    implement the enclosing interface.
