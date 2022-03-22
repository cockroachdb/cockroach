// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdecomp

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// descriptorStatus tries to map a descriptor to an element status for its
// top-level element on a best-effort basis. This is necessary incomplete as
// we can't distinguish between TXN_DROPPED and DROPPED for dropped descriptors.
//
// TODO(postamar): handle offline descriptors?
func descriptorStatus(desc catalog.Descriptor) scpb.Status {
	if desc.Dropped() {
		return scpb.Status_DROPPED
	}
	return scpb.Status_PUBLIC
}

// maybeMutationStatus tries to map a table mutation to an element status
// on a best-effort basis. This is necessary incomplete, for instance we can't
// distinguish between DELETE_AND_WRITE_ONLY, BACKFILLED and VALIDATED for
// write-only indexes.
//
// TODO(postamar): handle constraint mutation statuses
func maybeMutationStatus(mm catalog.TableElementMaybeMutation) scpb.Status {
	switch {
	case mm.DeleteOnly():
		return scpb.Status_DELETE_ONLY
	case mm.WriteAndDeleteOnly():
		return scpb.Status_WRITE_ONLY
	case mm.Backfilling():
		return scpb.Status_BACKFILL_ONLY
	case mm.Merging():
		return scpb.Status_MERGE_ONLY
	default:
		return scpb.Status_PUBLIC
	}
}

// newExpression parses the expression and walks its AST to collect all by-ID
// type and sequence references into an scpb.Expression expression wrapper.
func (w *walkCtx) newExpression(expr string) (*scpb.Expression, error) {
	e, err := parser.ParseExpr(expr)
	if err != nil {
		return nil, err
	}
	var seqIDs catalog.DescriptorIDSet
	{
		seqIdents, err := seqexpr.GetUsedSequences(e)
		if err != nil {
			return nil, err
		}
		for _, si := range seqIdents {
			if !si.IsByID() {
				panic(scerrors.NotImplementedErrorf(nil, /* n */
					"sequence %q referenced by name", si.SeqName))
			}
			seqIDs.Add(descpb.ID(si.SeqID))
		}
	}
	var typIDs catalog.DescriptorIDSet
	{
		visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
		tree.WalkExpr(visitor, e)
		for oid := range visitor.OIDs {
			if !types.IsOIDUserDefinedType(oid) {
				continue
			}
			id, err := typedesc.UserDefinedTypeOIDToID(oid)
			if err != nil {
				// This should never happen.
				return nil, err
			}
			if _, found := w.cachedTypeIDClosures[id]; !found {
				desc := w.lookupFn(id)
				typ, err := catalog.AsTypeDescriptor(desc)
				if err != nil {
					return nil, err
				}
				w.cachedTypeIDClosures[id], err = typ.GetIDClosure()
				if err != nil {
					return nil, err
				}
			}
			for id = range w.cachedTypeIDClosures[id] {
				typIDs.Add(id)
			}
		}
	}
	return &scpb.Expression{
		Expr:            catpb.Expression(expr),
		UsesTypeIDs:     typIDs.Ordered(),
		UsesSequenceIDs: seqIDs.Ordered(),
	}, nil
}

func newTypeT(t *types.T) (*scpb.TypeT, error) {
	ids, err := typedesc.GetTypeDescriptorClosure(t)
	if err != nil {
		return nil, err
	}
	var ret catalog.DescriptorIDSet
	for id := range ids {
		ret.Add(id)
	}
	ret.Remove(descpb.InvalidID)
	return &scpb.TypeT{
		Type:          t,
		ClosedTypeIDs: ret.Ordered(),
	}, nil
}
