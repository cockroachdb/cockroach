// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	d "github.com/cockroachdb/cockroach/pkg/sql/ir/example/base"
)

func makeSampleExpr(a d.Allocator) d.Expr {
	c1 := d.ConstExprValue{Datum: 1}.R(a).Expr()
	c2 := d.ConstExprValue{Datum: 2}.R(a).Expr()
	c3 := d.ConstExprValue{Datum: 3}.R(a).Expr()
	b4 := d.BinExprValue{Left: c1, Op: d.BinOpAdd, Right: c2}.R(a).Expr()
	b5 := d.BinExprValue{Left: c3, Op: d.BinOpMul, Right: b4}.R(a).Expr()
	return b5
}

func format(ref d.Expr) string {
	switch ref.Tag() {
	case d.ExprConstExpr:
		c := ref.MustBeConstExpr()
		return strconv.FormatInt(c.Datum(), 10)
	case d.ExprBinExpr:
		b := ref.MustBeBinExpr()
		var op string
		switch b.Op() {
		case d.BinOpAdd:
			op = "+"
		case d.BinOpMul:
			op = "*"
		default:
			panic("unknown BinOp")
		}
		return fmt.Sprintf("(%s %s %s)", format(b.Left()), op, format(b.Right()))
	default:
		panic("unknown Expr tag")
	}
}

func TestExprFormatSExpr(t *testing.T) {
	a := d.NewAllocator()
	e := makeSampleExpr(a)

	var buf bytes.Buffer
	e.FormatSExpr(&buf)
	if buf.String() != "(BinExpr Left: (ConstExpr Datum: 3) Op: Mul Right: (BinExpr Left: (ConstExpr Datum: 1) Op: Add Right: (ConstExpr Datum: 2)))" {
		t.Fatalf("unexpected: %q", buf.String())
	}
}

func TestFormat(t *testing.T) {
	a := d.NewAllocator()
	e := makeSampleExpr(a)
	const expected = "(3 * (1 + 2))"
	if got := format(e); got != expected {
		t.Fatalf("expected %q but got %q", expected, got)
	}
}

func reverse(ref d.Expr, a d.Allocator) d.Expr {
	if ref.Tag() != d.ExprBinExpr {
		return ref
	}
	b := ref.MustBeBinExpr()
	revLeft := reverse(b.Left(), a)
	revRight := reverse(b.Right(), a)
	return b.V().WithLeft(revRight).WithRight(revLeft).R(a).Expr()
}

func TestReverse(t *testing.T) {
	a := d.NewAllocator()
	e := makeSampleExpr(a)
	const expected = "((2 + 1) * 3)"
	if got := format(reverse(e, a)); got != expected {
		t.Fatalf("expected %q but got %q", expected, got)
	}
}

func TestDoubleReverse(t *testing.T) {
	a := d.NewAllocator()
	e := makeSampleExpr(a)
	if got0, got2 := format(e), format(reverse(reverse(e, a), a)); got0 != got2 {
		t.Fatalf("reverse is not an involution: %q != %q", got0, got2)
	}
}

func deepEqual(ref1 d.Expr, ref2 d.Expr) bool {
	if ref1.Tag() != ref2.Tag() {
		return false
	}
	switch ref1.Tag() {
	case d.ExprConstExpr:
		return ref1.MustBeConstExpr().Datum() == ref2.MustBeConstExpr().Datum()
	case d.ExprBinExpr:
		b1 := ref1.MustBeBinExpr()
		b2 := ref2.MustBeBinExpr()
		return b1.Op() == b2.Op() &&
			deepEqual(b1.Left(), b2.Left()) && deepEqual(b1.Right(), b2.Right())
	default:
		panic("unknown Expr tag")
	}
}

func TestDeepEqual(t *testing.T) {
	a := d.NewAllocator()
	e := makeSampleExpr(a)
	if deepEqual(e, reverse(e, a)) {
		t.Fatalf("expected expression not to be deepEqual to its reverse")
	}
	if !deepEqual(e, reverse(reverse(e, a), a)) {
		t.Fatalf("expected expression to be deepEqual to the reverse of its reverse")
	}
}
