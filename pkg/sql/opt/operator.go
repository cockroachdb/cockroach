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

	// TODO(radu): no relational operators yet.

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

	isDistinctFromOp
	isNotDistinctFromOp

	// isOp implements the SQL operator IS, as well as its extended
	// version IS NOT DISTINCT FROM.
	isOp

	// isNotOp implements the SQL operator IS NOT, as well as its extended
	// version IS DISTINCT FROM.
	isNotOp

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

	unaryPlusOp
	unaryMinusOp
	unaryComplementOp

	functionCallOp

	// This should be last.
	numOperators
)

// operatorInfo stores static information about an operator.
type operatorInfo struct {
	// name of the operator, used when printing expressions.
	name string
	// class of the operator (see operatorClass).
	class operatorClass

	normalizeFn func(*expr)
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
}

// operatorClass implements functionality that is common for a subset of
// operators.
type operatorClass interface {
	// format outputs information about the expr tree to a treePrinter.
	format(e *expr, tp treeprinter.Node)
}
