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

import "fmt"

type operator uint8

const (
	unknownOp operator = iota

	// TODO(radu): no relational operators yet.

	// Scalar operators
	variableOp
	constOp
	listOp
	orderedListOp

	existsOp

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
	isOp
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

	functionOp

	numOperators
)

type operatorInfo struct {
	name  string
	class operatorClass
}

var operatorTab = [numOperators]operatorInfo{
	unknownOp: {name: "unknown"},
}

func (op operator) String() string {
	if op >= numOperators {
		return fmt.Sprintf("operator(%d)", op)
	}
	return operatorTab[op].name
}

func registerOperator(op operator, name string, class operatorClass) {
	operatorTab[op].name = name
	operatorTab[op].class = class
}

type operatorClass interface {
	// format outputs information about the expr tree to a treePrinter.
	format(e *expr, tp *treePrinter)
}
