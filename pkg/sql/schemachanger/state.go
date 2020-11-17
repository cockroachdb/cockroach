package schemachanger

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type schemaChangerState struct {
	stmts    []*stmtInfo
	elements []element
}

// An ID scoped to a given transaction and thus
// schema changer.
type stmtID int

type stmtInfo struct {
	id   stmtID
	stmt tree.Statement
}

type element interface {
	stmtID() stmtID
	// ...
}

type addIndex struct {
	statementID stmtID

	tableID descpb.ID
	indexID descpb.IndexID
	// TODO(ajwerner): Consider adding column IDs to track dependencies at
	// step compile time.

	// Refer to a descriptor
	// And column ID
	// And maybe higher level change?
	state elemState
}

func (a addIndex) stmtID() stmtID {
	return a.statementID
}

type addColumn struct {
	statementID stmtID

	tableID  descpb.ID
	columnID descpb.ColumnID

	// Refer to a descriptor
	// And column ID
	// And maybe higher level change?
	state elemState
}

func (a addColumn) stmtID() stmtID {
	return a.statementID
}

type elemState int

const (
	elemDeleteOnly elemState = iota
	elemDeleteAndWriteOnly
	elemBackfilled
	elemPublic
	elemRemoved
)

func compileStateToForwardSteps(ctx context.Context, st schemaChangerState) ([]step, error) {
	// Elements to sequence of steps per element, then will combine.

	// elem: addColumn{state: elemDeleteOnly}
	// ops:
	//  - addColumnChangeStateOp{nextState: elemDeleteAndWriteOnly}
	//  - columnBackfillOp
	//  - addColumnChangeStateOp{nextState: elemPublic}

	// elem: addUniqueIndex{state: addIndexDeleteOnly}
	// ops:
	//  - addIndexChangeStateOp{nextState: elemDeleteAndWriteOnly}
	//  - indexBackfillOp
	//  - addIndexChangeStateOp{nextState: elemAdded}
	//  - uniqueIndexValidateOp
	//  - addIndexChangeStateOp{nextState: elemPublic}

	elemOps := make(map[element][]op)
	for _, elem := range st.elements {
		var err error
		elemOps[elem], err = compileOps(elem)
		if err != nil {
			return nil, err
		}
	}
	// Combine elemOps
	// panic("unimplemented")
	var result []step
	if len(elemOps) != 1 {
		panic("unimplemented")
	}
	var ops []op
	for _, ops = range elemOps {
		break
	}
	for _, op := range ops {
		switch t := op.(type) {
		case descriptorMutationOp:
			result = append(result, descriptorMutationOps{t})
		case validationOp:
			result = append(result, validationOps{t})
		case backfillOp:
			result = append(result, backfillOps{t})
		}
	}
	return result, nil
}

func compileOps(e element) ([]op, error) {
	switch e := e.(type) {
	case *addColumn:
		return compileAddColumnOps(e)
	default:
		return nil, errors.AssertionFailedf("unknown element type %T", e)
	}

}

func compileAddColumnOps(e *addColumn) ([]op, error) {
	var ops []op
	descChange := func(nextState elemState) *addColumnChangeStateOp {
		return &addColumnChangeStateOp{
			tableID:   e.tableID,
			columnID:  e.columnID,
			nextState: nextState,
		}
	}
	switch e.state {
	case elemDeleteOnly:
		ops = append(ops, descChange(elemDeleteAndWriteOnly))
		fallthrough
	case elemDeleteAndWriteOnly:
		ops = append(ops, &columnBackfillOp{
			tableID:            e.tableID,
			storedColumnsToAdd: []descpb.ColumnID{e.columnID},
		})
		fallthrough
	case elemBackfilled:
		ops = append(ops, descChange(elemPublic))
	case elemPublic:
	// no-op
	case elemRemoved:
		return nil, errors.AssertionFailedf("unexpected descriptor in %s state for %T", e.state, e)
	}
	return ops, nil
}
