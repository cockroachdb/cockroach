package main

import (
	"fmt"
	"strconv"
)

func Reverse(ref Expr, a *Allocator) Expr {
	if ref.Tag() != ExprBinExpr {
		return ref
	}
	b := ref.MustBeBinExpr()
	rl := Reverse(b.Left(), a)
	rr := Reverse(b.Right(), a)
	return b.V().WithLeft(rr).WithRight(rl).R(a).Expr()
}

func Format(ref Expr) string {
	switch ref.Tag() {
	case ExprConstExpr:
		c := ref.MustBeConstExpr()
		return strconv.FormatInt(c.Datum(), 10)
	case ExprBinExpr:
		b := ref.MustBeBinExpr()
		var op string
		switch b.Op() {
		case BinOpAdd:
			op = "+"
		case BinOpMul:
			op = "*"
		default:
			panic("unknown BinOp")
		}
		return fmt.Sprintf("(%s %s %s)", Format(b.Left()), op, Format(b.Right()))
	default:
		panic("unknown Expr tag")
	}
}

func DeepEqual(ref1 Expr, ref2 Expr) bool {
	if ref1.Tag() != ref2.Tag() {
		return false
	}
	switch ref1.Tag() {
	case ExprConstExpr:
		return ref1.MustBeConstExpr().Datum() == ref2.MustBeConstExpr().Datum()
	case ExprBinExpr:
		b1 := ref1.MustBeBinExpr()
		b2 := ref2.MustBeBinExpr()
		return b1.Op() == b2.Op() &&
			DeepEqual(b1.Left(), b2.Left()) && DeepEqual(b1.Right(), b2.Right())
	default:
		panic("unknown Expr tag")
	}
}

func main() {
	var a Allocator
	c1 := ConstExprValue{1}.R(&a).Expr()
	c2 := ConstExprValue{2}.R(&a).Expr()
	c3 := ConstExprValue{3}.R(&a).Expr()
	b4 := BinExprValue{c1, BinOpAdd, c2}.R(&a).Expr()
	b5 := BinExprValue{c3, BinOpMul, b4}.R(&a).Expr()
	println(Format(b5))
	e6 := Reverse(b5, &a)
	println(Format(e6))
	println(DeepEqual(e6, b5))
	e7 := Reverse(e6, &a)
	println(Format(e7))
	println(DeepEqual(e7, b5))
}
