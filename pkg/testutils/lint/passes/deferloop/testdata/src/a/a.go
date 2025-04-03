// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package a

import "fmt"

type Object struct {
	val int
}

func (o *Object) foo() {
	fmt.Println("foo")
}

func Basic(objects []*Object) {
	for _, o := range objects {
		defer o.foo() // want `defer inside loop`
		o.val++
	}
}

func Nested(objects []*Object) {
	for i, o := range objects {
		if i%2 == 0 {
			defer o.foo() // want `defer inside loop`
			o.val++
		}
	}
}

func InsideFuncOK(objects []*Object) {
	for _, o := range objects {
		func() {
			defer o.foo()
			o.val++
		}()
	}
}

func InsideFuncNotOK(objects []*Object) {
	for _, o := range objects {
		func() {
			for x := 1; x <= 10; x++ {
				defer o.foo() // want `defer inside loop`
				o.val++
			}
		}()
	}
}

func Nolint1(objects []*Object) {
	for _, o := range objects {
		//nolint:deferloop
		defer o.foo()
		o.val++
	}
}

func Nolint2(objects []*Object) {
	for _, o := range objects {
		defer o.foo() //nolint:deferloop
		o.val++
	}
}
