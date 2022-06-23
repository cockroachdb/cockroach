/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// ErrExprNotSupported signals that the expression cannot be handled by expression evaluation engine.
var ErrExprNotSupported = fmt.Errorf("Expr Not Supported")

//Convert converts between AST expressions and executable expressions
func Convert(e Expr) (evalengine.Expr, error) {
	switch node := e.(type) {
	case Argument:
		return evalengine.NewBindVar(string(node[1:])), nil
	case *Literal:
		switch node.Type {
		case IntVal:
			return evalengine.NewLiteralIntFromBytes(node.Val)
		case FloatVal:
			return evalengine.NewLiteralFloat(node.Val)
		case StrVal:
			return evalengine.NewLiteralString(node.Val), nil
		}
	case BoolVal:
		if node {
			return evalengine.NewLiteralIntFromBytes([]byte("1"))
		}
		return evalengine.NewLiteralIntFromBytes([]byte("0"))
	case *BinaryExpr:
		var op evalengine.BinaryExpr
		switch node.Operator {
		case PlusOp:
			op = &evalengine.Addition{}
		case MinusOp:
			op = &evalengine.Subtraction{}
		case MultOp:
			op = &evalengine.Multiplication{}
		case DivOp:
			op = &evalengine.Division{}
		default:
			return nil, ErrExprNotSupported
		}
		left, err := Convert(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := Convert(node.Right)
		if err != nil {
			return nil, err
		}
		return &evalengine.BinaryOp{
			Expr:  op,
			Left:  left,
			Right: right,
		}, nil

	}
	return nil, ErrExprNotSupported
}
