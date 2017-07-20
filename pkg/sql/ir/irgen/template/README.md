irgen templates
===============

This document describes briefly how irgen templates work.

This place is not a place of honor
----------------------------------

I (DE) didn’t want to develop a new template system, and neither should
you. Please don’t let this code turn into a nuclear waste dump.

My goal was a simple template that can be compiled as is, to reduce the
headache of debugging generated code. I couldn’t find an existing
template system that meets this requirement.

Note
----

This document is a literate Go program. To extract the Go samples from
this document:

``` {.shell}
sed -ne '/^```.*go/,/^```/{s/^```.*//
p
}' README.md >README.go
```

Here is the program header.

``` {.go}
package main

import (
    "bytes"
    "fmt"
    "os"

    "github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/template"
)
```

Motivation
----------

Let’s suppose that we need to generate structs with a constructor that
validates its arguments. The desired output is something like:

``` {.go}
type S1 struct {
    a int
}

func MakeS1(
    a int,
) S1 {
    if a < 0 {
        panic("invalid argument a")
    }
    return S1{
        a,
    }
}

type S2 struct {
    a int
    b int
}

func MakeS2(
    a int,
    b int,
) S2 {
    if a < 0 {
        panic("invalid argument a")
    }
    if b < 0 {
        panic("invalid argument b")
    }
    return S2{
        a,
        b,
    }
}
```

Let’s assume that the input is stored in slice of these structs.

``` {.go}
type Struct struct {
    Name   string
    Fields []string
}
```

The input to generate the code above would be:

``` {.go}
var input = []Struct{{"S1", []string{"a"}}, {"S2", []string{"a", "b"}}}
```

### Naive generation

We might try to generate code like this.

``` {.go}
func GenerateNaive(structs []Struct) string {
    var b bytes.Buffer
    for _, s := range structs {
        fmt.Fprintf(&b, "type %s struct {\n", s.Name)
        for _, f := range s.Fields {
            fmt.Fprintf(&b, "\t%s int\n", f)
        }
        fmt.Fprintf(&b, "}\n\n")
        fmt.Fprintf(&b, "func Make%s(\n", s.Name)
        for _, f := range s.Fields {
            fmt.Fprintf(&b, "\t%s int,\n", f)
        }
        fmt.Fprintf(&b, ") %s {\n", s.Name)
        for _, f := range s.Fields {
            fmt.Fprintf(&b, "\tif %s < 0 {\n", f)
            fmt.Fprintf(&b, "\t\tpanic(\"invalid argument %s\")\n", f)
            fmt.Fprintf(&b, "\t}\n")
        }
        fmt.Fprintf(&b, "\treturn %s{\n", s.Name)
        for _, f := range s.Fields {
            fmt.Fprintf(&b, "\t\t%s,\n", f)
        }
        fmt.Fprintf(&b, "\t}\n")
        fmt.Fprintf(&b, "}\n\n")
    }
    return b.String()
}
```

The first version of irgen code generation was written in this style and
rejected because it is not easy to change.

### Generation with an irgen template

For the irgen template, we write an example of each code block and
annotate it with directives indicating repetition.

``` {.go}
// @for struct

type StructName struct {
    // @for field
    Field int
    // @done field
}

func MakeStructName(
    // @for field
    Field int,
    // @done field
) StructName {
    // @for field
    if Field < 0 {
        panic("invalid argument Field")
    }
    // @done field
    return StructName{
        // @for field
        Field,
        // @done field
    }
}

// @done struct
```

Here is the code to instantiate the template (paste the code above to
stdin).

``` {.go}
func GenerateIrgen(structs []Struct) string {
    root := template.NewRoot()
    for _, s := range structs {
        node := root.NewChild("struct")
        node.AddReplacement("StructName", s.Name)
        for _, f := range s.Fields {
            child := node.NewChild("field")
            child.AddReplacement("Field", f)
        }
    }
    var b bytes.Buffer
    tmpl, _ := template.Parse(os.Stdin)
    tmpl.Instantiate(&b, root)
    return b.String()
}
```

Additional features
-------------------

In addition to `@for label`/`@done label`, there is
`@if label`/`@fi label`. The block is processed either zero or one times
depending on whether there exists a child node with the specified label.

The first argument to `AddReplacement` is actually a regexp. The first
and second arguments are passed to `ReplaceAll` in package `regexp`.

The second argument `AddReplacement` is an empty interface that is
converted to a string using a `%s` conversion. For other formats and/or
arbitrary argument counts, use `AddReplacementf`.

Replacements are processed in reverse order so that macro-style
substitutions are convenient.

Coda
----

Here is the main function.

``` {.go}
func main() {
    fmt.Println(GenerateNaive(input))
    fmt.Println(GenerateIrgen(input))
}
```
