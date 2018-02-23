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

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type operator uint8

const (
	unknownOp operator = iota

	// -- Scalar operators --

	// variableOp is a leaf expression that represents a non-constant value, like a column
	// in a table.
	variableOp

	// constOp is a leaf expression that has a constant value.
	constOp

	// tupleOp is a list of scalar expressions.
	tupleOp

	andOp
	orOp
	notOp

	eqOp
	ltOp
	gtOp
	leOp
	geOp
	neOp
	inOp
	notInOp
	likeOp
	notLikeOp
	iLikeOp
	notILikeOp
	similarToOp
	notSimilarToOp
	regMatchOp
	notRegMatchOp
	regIMatchOp
	notRegIMatchOp

	// isOp implements the SQL operator IS, as well as its extended
	// version IS NOT DISTINCT FROM.
	isOp

	// isNotOp implements the SQL operator IS NOT, as well as its extended
	// version IS DISTINCT FROM.
	isNotOp

	// containsOp is the @> JSON operator.
	containsOp
	// containedByOp is the <@ JSON operator.
	containedByOp
	// jsonExistsOp is the ? JSON operator.
	jsonExistsOp
	// jsonAllExistsOp is the ?& JSON operator.
	jsonAllExistsOp
	// jsonSomeExistsOp is the ?| JSON operator.
	jsonSomeExistsOp

	anyOp
	someOp
	allOp

	bitandOp
	bitorOp
	bitxorOp
	plusOp
	minusOp
	multOp
	divOp
	floorDivOp
	modOp
	powOp
	concatOp
	lShiftOp
	rShiftOp
	jsonFetchValOp
	jsonFetchTextOp
	jsonFetchValPathOp
	jsonFetchTextPathOp

	unaryPlusOp
	unaryMinusOp
	unaryComplementOp

	functionCallOp

	// unsupportedScalarOp is a temporary facility to pass through an unsupported
	// TypedExpr (like a subquery) through MakeIndexConstraints.
	unsupportedScalarOp

	// This should be last.
	numOperators
)

// operatorInfo stores static information about an operator.
type operatorInfo struct {
	// name of the operator, used when printing expressions.
	name string
	// class of the operator (see operatorClass).
	class operatorClass
	// operator-specific layout of auxiliary expressions.
	layout exprLayout

	normalizeFn func(*Expr)
}

// operatorTab stores static information about all operators.
var operatorTab = [numOperators]operatorInfo{
	unknownOp: {name: "unknown"},
}

func (op operator) String() string {
	if op >= numOperators {
		return fmt.Sprintf("operator(%d)", op)
	}
	return operatorTab[op].name
}

// registerOperator initializes the operator's entry in operatorTab.
// There must be a call to registerOperator in an init() function for every
// operator.
func registerOperator(op operator, info operatorInfo) {
	operatorTab[op] = info

	if info.class != nil {
		// Normalize the layout so that auxiliary expressions that are not present
		// are given an invalid index which will cause a panic if they are accessed.
		l := info.class.layout()
		if l.numAux == 0 {
			if l.aggregations == 0 {
				l.aggregations = -1
			} else {
				l.numAux++
			}
			if l.groupings == 0 {
				l.groupings = -1
			} else {
				l.numAux++
			}
			if l.projections == 0 {
				l.projections = -1
			} else {
				l.numAux++
			}
			if l.filters == 0 {
				l.filters = -1
			} else {
				l.numAux++
			}
		}
		operatorTab[op].layout = l
	}
}

// operatorClass implements functionality that is common for a subset of
// operators.
type operatorClass interface {
	// format outputs information about the expr tree to a treePrinter.
	format(e *Expr, tp treeprinter.Node)

	// layout returns the operator-specific expression layout.
	layout() exprLayout
}
