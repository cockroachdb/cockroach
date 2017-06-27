package main

import "fmt"

type Allocator struct {
	nodes []arb
}

func (a *Allocator) New() *arb {
	if len(a.nodes) == 0 {
		a.nodes = make([]arb, 256)
	}
	node := &a.nodes[0]
	a.nodes = a.nodes[1:]
	return node
}

type arb struct {
	enums [1]int32
	refs [2]*arb
	extra
}

type extra interface {
	extraRefs() []*arb
}

// ---- Expr ---- //

type Expr struct {
	tag ExprTag
	ref *arb
}

type ExprTag int32

const (
	ExprConstExpr ExprTag = 1
	ExprBinExpr = 2
)

func (x ExprTag) String() string {
	switch x {
	case ExprConstExpr:
		return "ConstExpr"
	case ExprBinExpr:
		return "BinExpr"
	default:
		return fmt.Sprintf("<unknown ExprTag %d>", x)
	}
}

func (x Expr) Tag() ExprTag { return x.tag }

func (x ConstExpr) Expr() Expr { return Expr{ExprConstExpr, x.ref} }

func (x Expr) ConstExpr() (ConstExpr, bool) {
	if x.tag != ExprConstExpr {
		return ConstExpr{}, false
	}
	return ConstExpr{x.ref}, true
}

func (x Expr) MustBeConstExpr() ConstExpr {
	if x.tag != ExprConstExpr {
		panic(fmt.Sprintf("type assertion failed: expected ConstExpr but got %s", x.tag))
	}
	return ConstExpr{x.ref}
}

func (x BinExpr) Expr() Expr { return Expr{ExprBinExpr, x.ref} }

func (x Expr) BinExpr() (BinExpr, bool) {
	if x.tag != ExprBinExpr {
		return BinExpr{}, false
	}
	return BinExpr{x.ref}, true
}

func (x Expr) MustBeBinExpr() BinExpr {
	if x.tag != ExprBinExpr {
		panic(fmt.Sprintf("type assertion failed: expected BinExpr but got %s", x.tag))
	}
	return BinExpr{x.ref}
}

// ---- ConstExpr ---- //

type ConstExpr struct{ ref *arb }

type ConstExprValue struct {
	Datum int64 // 1
}

type extraConstExpr struct {
	Datum int64
}

func (x extraConstExpr) extraRefs() []*arb { return nil }

func (x ConstExprValue) R(a *Allocator) ConstExpr {
	y := a.New()
	var extra extraConstExpr
	extra.Datum = x.Datum
	y.extra = extra
	return ConstExpr{y}
}

func (x ConstExpr) Datum() int64 { return x.ref.extra.(extraConstExpr).Datum }

func (x ConstExpr) V() ConstExprValue {
	return ConstExprValue{x.Datum()}
}

func (x ConstExprValue) WithDatum(y int64) ConstExprValue {
	x.Datum = y
	return x
}

// ---- BinExpr ---- //

type BinExpr struct{ ref *arb }

type BinExprValue struct {
	Left Expr // 1
	Op BinOp // 2
	Right Expr // 3
}

type extraBinExpr struct {
	Op BinOp
	Right ExprTag
}

func (x extraBinExpr) extraRefs() []*arb { return nil }

func (x BinExprValue) R(a *Allocator) BinExpr {
	y := a.New()
	var extra extraBinExpr
	y.enums[0] = int32(x.Left.tag)
	y.refs[0] = x.Left.ref
	extra.Op = x.Op
	extra.Right = x.Right.tag
	y.refs[1] = x.Right.ref
	y.extra = extra
	return BinExpr{y}
}

func (x BinExpr) Left() Expr { return Expr{ExprTag(x.ref.enums[0]), x.ref.refs[0]} }

func (x BinExpr) Op() BinOp { return x.ref.extra.(extraBinExpr).Op }

func (x BinExpr) Right() Expr { return Expr{x.ref.extra.(extraBinExpr).Right, x.ref.refs[1]} }

func (x BinExpr) V() BinExprValue {
	return BinExprValue{x.Left(), x.Op(), x.Right()}
}

func (x BinExprValue) WithLeft(y Expr) BinExprValue {
	x.Left = y
	return x
}

func (x BinExprValue) WithOp(y BinOp) BinExprValue {
	x.Op = y
	return x
}

func (x BinExprValue) WithRight(y Expr) BinExprValue {
	x.Right = y
	return x
}

// ---- BinOp ---- //

type BinOp int32

const (
	BinOpAdd BinOp = 1
	BinOpMul = 2
)

func (x BinOp) String() string {
	switch x {
	case BinOpAdd:
		return "Add"
	case BinOpMul:
		return "Mul"
	default:
		return fmt.Sprintf("<unknown BinOp %d>", x)
	}
}
