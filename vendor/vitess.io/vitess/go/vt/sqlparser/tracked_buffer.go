/*
Copyright 2019 The Vitess Authors.

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
	"strings"
)

// NodeFormatter defines the signature of a custom node formatter
// function that can be given to TrackedBuffer for code generation.
type NodeFormatter func(buf *TrackedBuffer, node SQLNode)

// TrackedBuffer is used to rebuild a query from the ast.
// bindLocations keeps track of locations in the buffer that
// use bind variables for efficient future substitutions.
// nodeFormatter is the formatting function the buffer will
// use to format a node. By default(nil), it's FormatNode.
// But you can supply a different formatting function if you
// want to generate a query that's different from the default.
type TrackedBuffer struct {
	*strings.Builder
	bindLocations []bindLocation
	nodeFormatter NodeFormatter
}

// NewTrackedBuffer creates a new TrackedBuffer.
func NewTrackedBuffer(nodeFormatter NodeFormatter) *TrackedBuffer {
	return &TrackedBuffer{
		Builder:       new(strings.Builder),
		nodeFormatter: nodeFormatter,
	}
}

// WriteNode function, initiates the writing of a single SQLNode tree by passing
// through to Myprintf with a default format string
func (buf *TrackedBuffer) WriteNode(node SQLNode) *TrackedBuffer {
	buf.Myprintf("%v", node)
	return buf
}

// Myprintf mimics fmt.Fprintf(buf, ...), but limited to Node(%v),
// Node.Value(%s) and string(%s). It also allows a %a for a value argument, in
// which case it adds tracking info for future substitutions.
// It adds parens as needed to follow precedence rules when printing expressions.
// To handle parens correctly for left associative binary operators,
// use %l and %r to tell the TrackedBuffer which value is on the LHS and RHS
//
// The name must be something other than the usual Printf() to avoid "go vet"
// warnings due to our custom format specifiers.
// *** THIS METHOD SHOULD NOT BE USED FROM ast.go. USE astPrintf INSTEAD ***
func (buf *TrackedBuffer) Myprintf(format string, values ...interface{}) {
	buf.astPrintf(nil, format, values...)
}

// astPrintf is for internal use by the ast structs
func (buf *TrackedBuffer) astPrintf(currentNode SQLNode, format string, values ...interface{}) {
	currentExpr, checkParens := currentNode.(Expr)
	if checkParens {
		// expressions that have Precedence Syntactic will never need parens
		checkParens = precedenceFor(currentExpr) != Syntactic
	}

	end := len(format)
	fieldnum := 0
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' {
			i++
		}
		if i > lasti {
			buf.WriteString(format[lasti:i])
		}
		if i >= end {
			break
		}
		i++ // '%'
		token := format[i]
		switch token {
		case 'c':
			switch v := values[fieldnum].(type) {
			case byte:
				buf.WriteByte(v)
			case rune:
				buf.WriteRune(v)
			default:
				panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
			}
		case 's':
			switch v := values[fieldnum].(type) {
			case []byte:
				buf.Write(v)
			case string:
				buf.WriteString(v)
			default:
				panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
			}
		case 'l', 'r', 'v':
			left := token != 'r'
			value := values[fieldnum]
			expr := getExpressionForParensEval(checkParens, value)

			if expr == nil {
				buf.formatter(value.(SQLNode))
			} else {
				needParens := needParens(currentExpr, expr, left)
				buf.printIf(needParens, "(")
				buf.formatter(expr)
				buf.printIf(needParens, ")")
			}
		case 'a':
			buf.WriteArg(values[fieldnum].(string))
		default:
			panic("unexpected")
		}
		fieldnum++
		i++
	}
}

func getExpressionForParensEval(checkParens bool, value interface{}) Expr {
	if checkParens {
		expr, isExpr := value.(Expr)
		if isExpr {
			return expr
		}
	}
	return nil
}

func (buf *TrackedBuffer) printIf(condition bool, text string) {
	if condition {
		buf.WriteString(text)
	}
}

func (buf *TrackedBuffer) formatter(node SQLNode) {
	if buf.nodeFormatter == nil {
		node.Format(buf)
	} else {
		buf.nodeFormatter(buf, node)
	}
}

//needParens says if we need a parenthesis
// op is the operator we are printing
// val is the value we are checking if we need parens around or not
// left let's us know if the value is on the lhs or rhs of the operator
func needParens(op, val Expr, left bool) bool {
	// Values are atomic and never need parens
	if IsValue(val) {
		return false
	}

	if areBothISExpr(op, val) {
		return true
	}

	opBinding := precedenceFor(op)
	valBinding := precedenceFor(val)

	if opBinding == Syntactic || valBinding == Syntactic {
		return false
	}

	if left {
		// for left associative operators, if the value is to the left of the operator,
		// we only need parens if the order is higher for the value expression
		return valBinding > opBinding
	}

	return valBinding >= opBinding
}

func areBothISExpr(op Expr, val Expr) bool {
	_, isOpIS := op.(*IsExpr)
	if isOpIS {
		_, isValIS := val.(*IsExpr)
		if isValIS {
			// when using IS on an IS op, we need special handling
			return true
		}
	}
	return false
}

// WriteArg writes a value argument into the buffer along with
// tracking information for future substitutions. arg must contain
// the ":" or "::" prefix.
func (buf *TrackedBuffer) WriteArg(arg string) {
	buf.bindLocations = append(buf.bindLocations, bindLocation{
		offset: buf.Len(),
		length: len(arg),
	})
	buf.WriteString(arg)
}

// ParsedQuery returns a ParsedQuery that contains bind
// locations for easy substitution.
func (buf *TrackedBuffer) ParsedQuery() *ParsedQuery {
	return &ParsedQuery{Query: buf.String(), bindLocations: buf.bindLocations}
}

// HasBindVars returns true if the parsed query uses bind vars.
func (buf *TrackedBuffer) HasBindVars() bool {
	return len(buf.bindLocations) != 0
}

// BuildParsedQuery builds a ParsedQuery from the input.
func BuildParsedQuery(in string, vars ...interface{}) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf(in, vars...)
	return buf.ParsedQuery()
}
