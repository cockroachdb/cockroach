package demo

// In this test, we're going to show mutations performed in-place
// as well as mutations performed by replacement.  We have visitable
// types *Foo and Quux.  We can modify *Foo in place, but must
// replace values of Quux.

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type Printer struct {
	StatementVisitorBase
	w strings.Builder
}

var _ StatementVisitor = &Printer{}

func (p *Printer) PreFoo(ctx StatementContext, foo *Foo) (bool, error) {
	p.w.WriteString(foo.val)
	return false, nil
}

func (p *Printer) PreQuux(ctx StatementContext, x Quux) (bool, error) {
	p.w.WriteString(x.now.String())
	return false, nil
}

type Mutator struct {
	StatementVisitorBase
}

var _ StatementVisitor = &Mutator{}

// We're going to mutate Foo's in-place.
func (Mutator) PreFoo(ctx StatementContext, foo *Foo) (bool, error) {
	// Via Russ Cox
	// https://groups.google.com/d/msg/golang-nuts/oPuBaYJ17t4/PCmhdAyrNVkJ
	n := 0
	runes := make([]rune, len(foo.val))
	for _, r := range foo.val {
		runes[n] = r
		n++
	}
	// Account for multi-byte points.
	runes = runes[0:n]
	// Reverse.
	for i := 0; i < n/2; i++ {
		runes[i], runes[n-1-i] = runes[n-1-i], runes[i]
	}

	// Update in-place.
	foo.val = string(runes)
	return false, nil
}

// We're going to replace Quux instances.
func (Mutator) PostQuux(ctx StatementContext, quux Quux) error {
	quux.now = timeutil.Now()
	ctx.Replace(quux)
	// Just to be explicit that once the replacement has happened,
	// it's all by-value.
	quux.now = timeutil.UnixEpoch
	return nil
}

func TestPrint(t *testing.T) {
	x := Bar{
		foo: Foo{
			val: "olleH",
		},
		fooPtr: &Foo{
			val: "!dlroW ",
		},
		quux: Quux{
			now: timeutil.UnixEpoch,
		},
		quuxPtr: &Quux{
			now: timeutil.UnixEpoch,
		},
	}

	x2, changed, err := x.Walk(context.Background(), Mutator{})
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("not changed")
	}
	if x.fooPtr != x2.fooPtr {
		t.Fatal("fooPtr should not have changed")
	}

	sv := &Printer{}
	x3, changed, err := x2.Walk(context.Background(), sv)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("should not have changed")
	}
	if x2.fooPtr != x3.fooPtr {
		t.Fatal("pointer should not have changed")
	}
	t.Log(sv.w.String())
}

func BenchmarkNoop(b *testing.B) {
	x := Bar{
		foo: Foo{
			val: "olleH",
		},
		fooPtr: &Foo{
			val: "!dlroW ",
		},
		quux: Quux{
			now: timeutil.UnixEpoch,
		},
		quuxPtr: &Quux{
			now: timeutil.UnixEpoch,
		},
	}
	v := &StatementVisitorBase{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := x.Walk(context.Background(), v); err != nil {
			b.Fatal(err)
		}
	}
}
