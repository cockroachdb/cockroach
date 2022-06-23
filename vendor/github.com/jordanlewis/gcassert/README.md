# gcassert

gcassert is a program for making assertions about compiler decisions in
Golang programs, via inline comment directives like `//gcassert:inline`.

Currently supported [directives](#directives):

- `//gcassert:inline` to assert function callsites are inlined
- `//gcassert:bce` to assert bounds checks are eliminated
- `//gcassert:noescape` to assert variables don't escape to the heap

## Example

Given a file `foo.go`:

```go
package foo

func addOne(i int) int {
    return i+1
}

//gcassert:inline
func addTwo(i int) int {
    return i+1
}

func a(ints []int) int {
    var sum int
    for i := range ints {
        //gcassert:bce,inline
        sum += addOne(ints[i])

        sum += addTwo(ints[i]) //gcassert:bce

        sum += ints[i] //gcassert:bce
    }
    return sum
}
```

The inline `//gcassert` directive will cause `gcassert` to fail if the line
`sum += addOne(ints[i])` is either not inlined or contains bounds checks.

A `//gcassert:inline` directive on a function will cause `gcassert` to fail
if any of the callers of that function do not get inlined.

`//gcassert` comments expect a comma-separated list of directives after
`//gcassert:`. They can be included above the line in question or after, as an
inline comment.

## Installation

To get the gcassert binary:

```bash
go get github.com/jordanlewis/gcassert/cmd/gcassert
```

To get the gcassert library:

```bash
go get github.com/jordanlewis/gcassert
```

## Usage

### As a binary

Run gcassert on packages containing gcassert directives, like this:

```bash
gcassert ./package/path
```

The program will output all lines that had a gcassert directive that wasn't
respected by the compiler.

For example, running on the testdata directory in this library will produce the
following output:

```bash
$ gcassert ./testdata
testdata/noescape.go:21:        foo := foo{a: 1, b: 2}: foo escapes to heap:
testdata/bce.go:8:      fmt.Println(ints[5]): Found IsInBounds
testdata/bce.go:17:     sum += notInlinable(ints[i]): call was not inlined
testdata/bce.go:19:     sum += notInlinable(ints[i]): call was not inlined
testdata/inline.go:45:  alwaysInlined(3): call was not inlined
testdata/inline.go:51:  sum += notInlinable(i): call was not inlined
testdata/inline.go:55:  sum += 1: call was not inlined
testdata/inline.go:58:  test(0).neverInlinedMethod(10): call was not inlined
```

Inspecting each of the listed lines will show a `//gcassert` directive
that wasn't upheld when running the compiler on the package.

### As a library

gcassert is runnable as a library as well, for integration into your linter
suite. It has a single package function, `gcassert.GCAssert`.

To use it, pass in an `io.Writer` to which errors will be written and a list of
paths to check for `gcassert` assertions, like this:

```go
package main

import "github.com/jordanlewis/gcassert"

func main() {
    var buf strings.Builder
    if err := gcassert.GCAssert(&buf, "./path/to/package", "./otherpath/to/package"); err != nil {
        // handle non-lint-failure related errors
        panic(err)
    }
    // Output the errors to stdout.
    fmt.Println(buf.String())
}
```

## Directives


```
//gcassert:inline
```

The inline directive on a CallExpr asserts that the following statement
contains a function that is inlined by the compiler. If the function does not
get inlined, gcassert will fail.

The inline directive on a FuncDecl asserts that every caller of that function
is actually inlined by the compiler.

```
//gcassert:bce
```

The bce directive asserts that the following statement contains a slice index
that has no necessary bounds checks. If the compiler adds bounds checks,
gcassert will fail.

```
//gcassert:noescape
```

The noescape directive asserts that the following variable does not escape to
the heap. If the compiler forces the variable to escape, gcassert will fail.
