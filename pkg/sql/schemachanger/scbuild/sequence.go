// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// dropSequenceDesc builds targets and transformations using a descriptor.
func (b *buildContext) dropSequenceDesc(
	ctx context.Context, seq catalog.TableDescriptor, cascade tree.DropBehavior,
) {
	if err := b.AuthAccessor.CheckPrivilege(ctx, seq, privilege.DROP); err != nil {
		panic(err)
	}
	// Add a node to drop the sequence
	b.decomposeTableDescToElements(ctx, seq, scpb.Target_DROP)
	// Check if there are dependencies.
	err := b.forEachNodeOfType(scpb.Target_DROP, reflect.TypeOf((*scpb.RelationDependedOnBy)(nil)),
		func(element scpb.Element) error {
			dep := element.(*scpb.RelationDependedOnBy)
			if dep.TableID != seq.GetID() {
				return nil
			}
			if cascade != tree.DropCascade {
				return pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"cannot drop sequence %s because other objects depend on it",
					seq.GetName(),
				)
			}
			desc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, dep.TableID, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return err
			}
			for _, col := range desc.PublicColumns() {
				if dep.GetXColumnID() == nil ||
					col.GetID() != dep.GetColumnID() {
					continue
				}
				// Convert the default expression into elements.
				b.decomposeDefaultExprToElements(desc, col, scpb.Target_DROP)
			}
			return nil
		})
	if err != nil {
		panic(err)
	}
}

// dropSequence builds targets and transforms the provided schema change nodes
// accordingly, given an DROP SEQUENCE statement.
func (b *buildContext) dropSequence(ctx context.Context, n *tree.DropSequence) {
	// Find the sequence first.
	for _, name := range n.Names {
		_, table, err := resolver.ResolveExistingTableObject(ctx, b.Res, &name,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			if pgerror.GetPGCode(err) == pgcode.UndefinedTable && n.IfExists {
				continue
			}
			panic(err)
		}
		if table == nil {
			panic(errors.AssertionFailedf("unable to resolve sequence %s",
				name.FQString()))
		}

		if table.Dropped() {
			return
		}
		b.dropSequenceDesc(ctx, table, n.DropBehavior)
	}
}

// createSequence builds targets and transforms the provided schema change nodes
// accordingly, given an CREATE SEQUENCE statement.
func (b *buildContext) createSequence(ctx context.Context, n *tree.CreateSequence) {
	_, prefix, err := resolver.ResolveObjectNamePrefix(ctx,
		b.Res,
		b.EvalCtx.SessionData().Database,
		b.EvalCtx.SessionData().SearchPath,
		&n.Name.ObjectNamePrefix)
	if err != nil {
		panic(err)
	}
	_, desc, err := resolver.ResolveExistingTableObject(ctx, b.Res, &n.Name,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required: false,
			},
		})
	if err != nil {
		panic(err)
	}
	// FIXME: Support If not exists...
	if desc != nil {
		panic(sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), n.Name.FQString()))
	}
	// Allocate the namespace and ID for this object.
	descID, err := catalogkv.GenerateUniqueDescID(ctx, b.EvalCtx.DB, b.EvalCtx.Codec)
	if err != nil {
		panic(err)
	}
	nameSpaceEntry := scpb.Namespace{
		DatabaseID:   prefix.Database.GetID(),
		SchemaID:     prefix.Schema.GetID(),
		DescriptorID: descID,
		Name:         n.Name.ObjectName.String(),
	}
	b.addNode(scpb.Target_ADD, &nameSpaceEntry)
	// Create a new sequence element.
	sequenceEntry := scpb.Sequence{
		SequenceID: descID,
	}
	b.addNode(scpb.Target_ADD, &sequenceEntry)
	// Loop over any sequence options and pick them up
	for _, option := range n.Options {
		/*	SeqOptAs        = "AS"
			SeqOptCycle     = "CYCLE"
			SeqOptNoCycle   = "NO CYCLE"
			SeqOptOwnedBy   = "OWNED BY"
			SeqOptCache     = "CACHE"
			SeqOptIncrement = "INCREMENT"
			SeqOptMinValue  = "MINVALUE"
			SeqOptMaxValue  = "MAXVALUE"
			SeqOptStart     = "START"
			SeqOptVirtual   = "VIRTUAL"*/
		switch option.Name {
		default:
			panic(option)
		}
	}

	// Add the column...
	sequenceColumnName := scpb.ColumnName{
		TableID:  descID,
		ColumnID: tabledesc.SequenceColumnID,
		Name:     tabledesc.SequenceColumnName,
	}
	b.addNode(scpb.Target_ADD, &sequenceColumnName)
	sequenceColumn := scpb.Column{
		TableID:    descID,
		ColumnID:   tabledesc.SequenceColumnID,
		Type:       types.Int,
		FamilyName: "primary",
		FamilyID:   keys.SequenceColumnFamilyID,
	}

	b.addNode(scpb.Target_ADD, &sequenceColumn)
	b.buildMetaData.CreatedDescriptors.Add(descID)
	/*	indexDescriptor := scpb.PrimaryIndex{
			TableID: descID,
			Index: descpb.IndexDescriptor{
				ID:                  keys.SequenceIndexID,
				Name:                tabledesc.PrimaryKeyIndexName,
				KeyColumnIDs:        []descpb.ColumnID{tabledesc.SequenceColumnID},
				KeyColumnNames:      []string{tabledesc.SequenceColumnName},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
			},
		}
		b.addNode(scpb.Target_ADD, &indexDescriptor)*/
	// FIXME: Commit the column mutations faster some how...
	// FIXME: Create a single column...
	// FIXME: Other information?
}
