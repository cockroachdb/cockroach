package schemachanger

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
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

func compileStateToForwardSteps(ctx context.Context, st schemaChangerState) ([]ops2.step, error) {
	// Elements to sequence of steps per element, then will combine.

	// elem: AddColumn{state: elemDeleteOnly}
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

	elemOps := make(map[element][]ops2.op)
	for _, elem := range st.elements {
		var err error
		elemOps[elem], err = compileOps(elem)
		if err != nil {
			return nil, err
		}
	}
	// Combine elemOps
	// panic("unimplemented")
	var result []ops2.step
	if len(elemOps) != 1 {
		panic("unimplemented")
	}
	var ops []ops2.op
	for _, ops = range elemOps {
		break
	}
	for _, op := range ops {
		switch t := op.(type) {
		case ops2.descriptorMutationOp:
			result = append(result, ops2.descriptorMutationOps{t})
		case ops2.validationOp:
			result = append(result, ops2.validationOps{t})
		case ops2.backfillOp:
			result = append(result, ops2.backfillOps{t})
		}
	}
	return result, nil
}

func compileOps(e element) ([]ops2.op, error) {
	switch e := e.(type) {
	case *targets.AddColumn:
		return compileAddColumnOps(e)
	default:
		return nil, errors.AssertionFailedf("unknown element type %T", e)
	}

}

func compileAddColumnOps(e *targets.AddColumn) ([]ops2.op, error) {
	var ops []ops2.op
	descChange := func(nextState targets.elemState) *ops2.addColumnChangeStateOp {
		return &ops2.addColumnChangeStateOp{
			tableID:   e.tableID,
			columnID:  e.columnID,
			nextState: nextState,
		}
	}
	switch e.state {
	case targets.elemDeleteOnly:
		ops = append(ops, descChange(targets.elemDeleteAndWriteOnly))
		fallthrough
	case targets.elemDeleteAndWriteOnly:
		ops = append(ops, &ops2.columnBackfillOp{
			tableID:            e.tableID,
			storedColumnsToAdd: []descpb.ColumnID{e.columnID},
		})
		fallthrough
	case targets.elemBackfilled:
		ops = append(ops, descChange(targets.elemPublic))
	case targets.elemPublic:
	// no-op
	case targets.elemRemoved:
		return nil, errors.AssertionFailedf("unexpected descriptor in %s state for %T", e.state, e)
	}
	return ops, nil
}
