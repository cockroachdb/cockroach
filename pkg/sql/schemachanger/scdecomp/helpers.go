// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scdecomp

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// descriptorStatus tries to map a descriptor to an element status for its
// top-level element on a best-effort basis.
func descriptorStatus(desc catalog.Descriptor) scpb.Status {
	if desc.Dropped() {
		return scpb.Status_DROPPED
	}
	if desc.Offline() {
		return scpb.Status_OFFLINE
	}
	return scpb.Status_PUBLIC
}

// maybeMutationStatus tries to map a table mutation to an element status
// on a best-effort basis. This is necessary incomplete, for instance we can't
// distinguish between WRITE_ONLY, BACKFILLED and VALIDATED for
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
					redact.Sprintf("sequence %q referenced by name", si.SeqName),
				))
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
			id := typedesc.UserDefinedTypeOIDToID(oid)
			if _, found := w.cachedTypeIDClosures[id]; !found {
				desc := w.lookupFn(id)
				typ, err := catalog.AsTypeDescriptor(desc)
				if err != nil {
					return nil, err
				}
				w.cachedTypeIDClosures[id] = typ.GetIDClosure()
			}
			w.cachedTypeIDClosures[id].ForEach(typIDs.Add)
		}
	}

	var referencedColumns catalog.TableColSet
	switch t := w.desc.(type) {
	case catalog.TableDescriptor:
		referencedColumns, err = schemaexpr.ExtractColumnIDs(t, e)
		if err != nil {
			return nil, err
		}
	case catalog.FunctionDescriptor:
		// newExpression is called only for DEFAULT expressions of function
		// parameters which cannot have column references. Column references
		// from the function body are handled in WrapFunctionBody.
	default:
		return nil, errors.AssertionFailedf("expected either TableDescriptor of FunctionDescriptor, found %T", t)
	}
	referencedFnIDs, err := schemaexpr.GetUDFIDs(e)
	if err != nil {
		return nil, err
	}
	return &scpb.Expression{
		Expr:                catpb.Expression(expr),
		UsesTypeIDs:         typIDs.Ordered(),
		UsesSequenceIDs:     seqIDs.Ordered(),
		UsesFunctionIDs:     referencedFnIDs.Ordered(),
		ReferencedColumnIDs: referencedColumns.Ordered(),
	}, nil
}

func newTypeT(t *types.T) *scpb.TypeT {
	return &scpb.TypeT{
		Type:          t,
		ClosedTypeIDs: typedesc.GetTypeDescriptorClosure(t).Ordered(),
		TypeName:      t.SQLString(),
	}
}

// NewElementCreationMetadata construct a `*scpb.ElementCreationMetadata`
// based on `clusterVersion`.
func NewElementCreationMetadata(
	clusterVersion clusterversion.ClusterVersion,
) *scpb.ElementCreationMetadata {
	return &scpb.ElementCreationMetadata{
		In_23_1OrLater: true,
		In_24_3OrLater: true,
	}
}
