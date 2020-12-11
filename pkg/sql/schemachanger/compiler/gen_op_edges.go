package compiler

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Deal with figuring out when we need to issue a GC job and for
// what.

// TODO(ajwerner): Deal with the flags to indicate whether things should be done
// in the current transaction.

func generateOpEdges(
	g *targetStateGraph, t targets.Target, s targets.State, flags CompileFlags,
) error {
	switch t := t.(type) {
	case *targets.AddColumn:
		return generateAddColumnOpEdges(g, t, s, flags)
	case *targets.AddCheckConstraint:
		return generateAddCheckConstraintOpEdges(g, t, s, flags)
	case *targets.DropIndex:
		return generateDropIndexOpEdges(g, t, s, flags)
	case *targets.AddIndex:
		return generateAddIndexOpEdges(g, t, s, flags)
	case *targets.DropColumn:
		return generateDropColumnOpEdges(g, t, s, flags)
	default:
		return errors.AssertionFailedf("generateOpEdges not implemented for %T", t)
	}
}

func generateDropColumnOpEdges(
	g *targetStateGraph, t *targets.DropColumn, s targets.State, flags CompileFlags,
) error {
	for {
		switch s {
		case targets.State_PUBLIC:
			s = g.addOpEdge(t, s,
				targets.State_DELETE_AND_WRITE_ONLY,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.ColumnID,
					State:     s,
					NextState: targets.State_DELETE_AND_WRITE_ONLY,
				})
		case targets.State_DELETE_AND_WRITE_ONLY:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				(flags.ExecutionPhase == PostStatementPhase ||
					flags.ExecutionPhase == PreCommitPhase) {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_ONLY,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.ColumnID,
					State:     s,
					NextState: targets.State_DELETE_ONLY,
				})
		case targets.State_DELETE_ONLY:
			s = g.addOpEdge(t, s,
				targets.State_ABSENT,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.ColumnID,
					State:     s,
					NextState: targets.State_ABSENT,
				})
		case targets.State_ABSENT:
			return nil
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}

func generateAddIndexOpEdges(
	g *targetStateGraph, t *targets.AddIndex, s targets.State, flags CompileFlags,
) error {
	for {
		switch s {
		case targets.State_ABSENT:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PostStatementPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_ONLY,
				ops.AddIndexDescriptor{
					TableID: t.TableID,
					Index:   t.Index,
					Primary: t.Primary,
				})
		case targets.State_DELETE_ONLY:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PreCommitPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_AND_WRITE_ONLY,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.Index.ID,
					IsPrimary: t.Primary,
					State:     s,
					NextState: targets.State_DELETE_AND_WRITE_ONLY,
				})
		case targets.State_DELETE_AND_WRITE_ONLY:
			// TODO(ajwerner): In the case of a primary index swap, we only need to
			// validate if the columns being used did not previously contain a unique
			// and NOT NULL constraints.
			var next targets.State
			if !t.Index.Unique {
				next = targets.State_VALIDATED
			} else {
				next = targets.State_BACKFILLED
			}
			s = g.addOpEdge(t, s, next, ops.IndexBackfill{
				TableID: t.TableID,
				IndexID: t.Index.ID,
			})
		case targets.State_BACKFILLED:
			s = g.addOpEdge(t, s,
				targets.State_VALIDATED,
				ops.UniqueIndexValidation{
					TableID:        t.TableID,
					PrimaryIndexID: t.PrimaryIndex,
					IndexID:        t.Index.ID,
				})
		case targets.State_VALIDATED:
			s = g.addOpEdge(t, s,
				targets.State_PUBLIC,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.Index.ID,
					IsPrimary: t.Primary,
					State:     s,
					NextState: targets.State_PUBLIC,
				})
		case targets.State_PUBLIC:
			return nil
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}

func generateDropIndexOpEdges(
	g *targetStateGraph, t *targets.DropIndex, s targets.State, flags CompileFlags,
) error {
	for {
		switch s {
		case targets.State_PUBLIC:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PostStatementPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_AND_WRITE_ONLY,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.IndexID,
					IsPrimary: t.ReplacedBy != 0,
					State:     s,
					NextState: targets.State_DELETE_AND_WRITE_ONLY,
				})
		case targets.State_DELETE_AND_WRITE_ONLY:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PreCommitPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_ONLY,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.IndexID,
					IsPrimary: t.ReplacedBy != 0,
					State:     s,
					NextState: targets.State_DELETE_ONLY,
				})
		case targets.State_DELETE_ONLY:
			s = g.addOpEdge(t, s,
				targets.State_ABSENT,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.IndexID,
					IsPrimary: t.ReplacedBy != 0,
					State:     s,
					NextState: targets.State_ABSENT,
				})
		case targets.State_ABSENT:
			return nil
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}

func generateAddCheckConstraintOpEdges(
	g *targetStateGraph, t *targets.AddCheckConstraint, s targets.State, flags CompileFlags,
) error {
	panic("unimplemented")
}

func generateAddColumnOpEdges(
	g *targetStateGraph, t *targets.AddColumn, s targets.State, flags CompileFlags,
) (_ error) {
	for {
		switch s {
		case targets.State_ABSENT:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PostStatementPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_ONLY,
				ops.AddColumnDescriptor{
					TableID: t.TableID,
					Column:  t.Column,
				})
		case targets.State_DELETE_ONLY:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PreCommitPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.State_DELETE_AND_WRITE_ONLY,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.Column.ID,
					State:     s,
					NextState: targets.State_DELETE_AND_WRITE_ONLY,
				})
		case targets.State_DELETE_AND_WRITE_ONLY:
			s = g.addOpEdge(t, s,
				targets.State_PUBLIC,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.Column.ID,
					State:     s,
					NextState: targets.State_PUBLIC,
				})
		case targets.State_PUBLIC:
			return
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}
