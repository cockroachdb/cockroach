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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

func maybeMutationStatus(mm catalog.TableElementMaybeMutation) scpb.Status {
	switch {
	case mm.DeleteOnly():
		return scpb.Status_DELETE_ONLY
	case mm.WriteAndDeleteOnly():
		return scpb.Status_DELETE_AND_WRITE_ONLY
	default:
		return scpb.Status_PUBLIC
	}
}

func newExpression(expr string) (*scpb.Expression, error) {
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
			seqIDs.Add(descpb.ID(si.SeqID))
		}
	}
	var typIDs catalog.DescriptorIDSet
	{
		visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
		tree.WalkExpr(visitor, e)
		for oid := range visitor.OIDs {
			id, err := typedesc.UserDefinedTypeOIDToID(oid)
			if err != nil {
				continue
			}
			typIDs.Add(id)
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
