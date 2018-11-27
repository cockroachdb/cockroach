package demo

// REVIEWERS: Start reading this file first, then check out
// generated_api.go for the bulk of the user-visible API.

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/walker"
)

// The user starts by defining a common base interface for their
// visitable types. It is permissible for a package to define
// multiple base types, such as `Statement` and `Expr`. Any symbols
// derived from a visitable interface will have the interface's name
// included for disambiguation, so prefer shorter names.
type Statement interface {
	// The generator looks for this magic type.
	walker.Interface

	// This method isn't special in any way, we just need some way to
	// distinguish types defined in this package as being assignable
	// to Statement. An `isStatement()` marker would also be just fine.
	Name() string
}

// Foo is a "pure value" type which contains no pointers or slices.
type Foo struct {
	val string
}

// Because *Foo implements statement, the user will get *Foo in
// the visitor API.
func (*Foo) Name() string { return "Foo" }

type Bar struct {
	foo    Foo
	fooPtr *Foo

	quux    Quux
	quuxPtr *Quux

	fooSlice    []Foo
	fooPtrSlice []*Foo
}

func (*Bar) Name() string { return "Bar" }

type Quux struct {
	now time.Time
}

// Quux implements the Statement interface with a value receiver,
// so the user will also see it by-value.
func (Quux) Name() string { return "Quux" }
