package demo

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestTree(t *testing.T) {
	x := &Bar{
		foo: Foo{
			val: "Hello",
		},
		fooPtr: &Foo{
			val: "World!",
		},
	}

	depth := 0
	var w strings.Builder
	v := &StatementVisitorBase{
		DefaultPre: func(ctx StatementContext, x Statement) (b bool, e error) {
			for i := 0; i < depth; i++ {
				if _, err := w.WriteString("  "); err != nil {
					return false, nil
				}
			}
			w.WriteString(fmt.Sprintf("Name: %s; Type: %s\n", x.Name(), reflect.TypeOf(x)))
			depth++
			return true, nil
		},
		DefaultPost: func(ctx StatementContext, x Statement) error {
			depth--
			return nil
		},
	}
	if x2, dirty, err := WalkStatement(context.Background(), x, v); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("%v %+v %+v", dirty, x, x2)
	}
	t.Logf("Output:\n%s", w.String())
}
