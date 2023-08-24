package newpkg

import "testing"

func TestFoo(t *testing.T) {
	if Foo(8) != 64 {
		t.Fatal("lol")
	}
}
