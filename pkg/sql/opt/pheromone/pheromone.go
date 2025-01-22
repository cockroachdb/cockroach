// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pheromone

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

func VisibleToPheromone(expr memo.RelExpr) bool {
	switch expr.(type) {
	case *memo.NormCycleTestRelExpr:
		return false
	case *memo.MemoCycleTestRelExpr:
		return false
	case *memo.ProjectExpr:
		return false
	case *memo.BarrierExpr:
		return false
	case *memo.DistributeExpr:
		return false
	case *memo.ExplainExpr:
		return false
	default:
		return true
	}
}

func BuildChildRequired(
	parent memo.RelExpr, required *physical.Pheromone, childIdx int,
) *physical.Pheromone {
	if required.Any() {
		return nil
	}

	if !VisibleToPheromone(parent) {
		return required
	}

	return required.Child(childIdx)
}

// TODO generate this
func PheromoneFromExpr(expr memo.RelExpr) *physical.Pheromone {
	op := expr.Op()
	fields := json.EmptyJSONObject
	var children [3]*physical.Pheromone
	switch t := expr.(type) {
	case *memo.NormCycleTestRelExpr:
		// invisible
		return nil
	case *memo.MemoCycleTestRelExpr:
		// invisible
		return nil
	case *memo.InsertExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.UpdateExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.UpsertExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.DeleteExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.LockExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ScanExpr:
	case *memo.PlaceholderScanExpr:
	case *memo.SequenceSelectExpr:
	case *memo.ValuesExpr:
	case *memo.LiteralValuesExpr:
	case *memo.SelectExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ProjectExpr:
		// invisible
		return PheromoneFromExpr(t.Input)
	case *memo.InvertedFilterExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.InnerJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.LeftJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.RightJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.FullJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.SemiJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.AntiJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.IndexJoinExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.LookupJoinExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.InvertedJoinExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.MergeJoinExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.ZigzagJoinExpr:
	case *memo.InnerJoinApplyExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.LeftJoinApplyExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.SemiJoinApplyExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.AntiJoinApplyExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.GroupByExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ScalarGroupByExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.DistinctOnExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.EnsureDistinctOnExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.UpsertDistinctOnExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.EnsureUpsertDistinctOnExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.UnionExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.IntersectExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.ExceptExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.UnionAllExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.IntersectAllExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.ExceptAllExpr:
		children[0] = PheromoneFromExpr(t.Left)
		children[1] = PheromoneFromExpr(t.Right)
	case *memo.LocalityOptimizedSearchExpr:
	case *memo.LimitExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.OffsetExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.TopKExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.Max1RowExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.OrdinalityExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ProjectSetExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.WindowExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.WithExpr:
		children[0] = PheromoneFromExpr(t.Binding)
		children[1] = PheromoneFromExpr(t.Main)
	case *memo.WithScanExpr:
	case *memo.RecursiveCTEExpr:
		children[0] = PheromoneFromExpr(t.Binding)
		children[1] = PheromoneFromExpr(t.Initial)
		children[2] = PheromoneFromExpr(t.Recursive)
	case *memo.VectorSearchExpr:
	case *memo.VectorPartitionSearchExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.BarrierExpr:
		// invisible
		return PheromoneFromExpr(t.Input)
	case *memo.FakeRelExpr:
	case *memo.CreateTableExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.CreateViewExpr:
	case *memo.CreateFunctionExpr:
	case *memo.CreateTriggerExpr:
	case *memo.ExplainExpr:
		// invisible
		return PheromoneFromExpr(t.Input)
	case *memo.ShowTraceForSessionExpr:
	case *memo.OpaqueRelExpr:
	case *memo.OpaqueMutationExpr:
	case *memo.OpaqueDDLExpr:
	case *memo.AlterTableSplitExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.AlterTableUnsplitExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.AlterTableUnsplitAllExpr:
	case *memo.AlterTableRelocateExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ControlJobsExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ControlSchedulesExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.CancelQueriesExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.CancelSessionsExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ExportExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.ShowCompletionsExpr:
	case *memo.CreateStatisticsExpr:
	case *memo.AlterRangeRelocateExpr:
		children[0] = PheromoneFromExpr(t.Input)
	case *memo.CallExpr:
	default:
		panic(errors.AssertionFailedf("unhandled type: %s", t.Op()))
	}
	return physical.PheromoneFromFields(op, fields, children)
}
