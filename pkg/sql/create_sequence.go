// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type createSequenceNode struct {
	n      *tree.CreateSequence
	dbDesc catalog.DatabaseDescriptor
}

func (p *planner) CreateSequence(ctx context.Context, n *tree.CreateSequence) (planNode, error) {
	un := n.Name.ToUnresolvedObjectName()
	dbDesc, _, prefix, err := p.ResolveTargetObject(ctx, un)
	if err != nil {
		return nil, err
	}
	n.Name.ObjectNamePrefix = prefix

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createSequenceNode{
		n:      n,
		dbDesc: dbDesc,
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createSequenceNode) ReadingOwnWrites() {}

func (n *createSequenceNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("sequence"))

	_, schemaID, err := getTableCreateParams(params, n.dbDesc.GetID(), n.n.Persistence, &n.n.Name)
	if err != nil {
		if sqlerrors.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}

	return doCreateSequence(
		params, n.n.String(), n.dbDesc, schemaID, &n.n.Name, n.n.Persistence, n.n.Options,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

// doCreateSequence performs the creation of a sequence in KV. The
// context argument is a string to use in the event log.
func doCreateSequence(
	params runParams,
	context string,
	dbDesc catalog.DatabaseDescriptor,
	schemaID descpb.ID,
	name *tree.TableName,
	persistence tree.Persistence,
	opts tree.SequenceOptions,
	jobDesc string,
) error {
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB, params.p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	privs := CreateInheritedPrivilegesFromDBDesc(dbDesc, params.SessionData().User())

	if persistence.IsTemporary() {
		telemetry.Inc(sqltelemetry.CreateTempSequenceCounter)
	}

	// creationTime is initialized to a zero value and populated at read time.
	// See the comment in desc.MaybeIncrementVersion.
	//
	// TODO(ajwerner): remove the timestamp from NewSequenceTableDesc, it's
	// currently relied on in import and restore code and tests.
	var creationTime hlc.Timestamp
	desc, err := NewSequenceTableDesc(
		params.ctx,
		name.Object(),
		opts,
		dbDesc.GetID(),
		schemaID,
		id,
		creationTime,
		privs,
		persistence,
		&params,
	)
	if err != nil {
		return err
	}

	// makeSequenceTableDesc already validates the table. No call to
	// desc.ValidateTable() needed here.

	key := catalogkv.MakeObjectNameKey(
		params.ctx,
		params.ExecCfg().Settings,
		dbDesc.GetID(),
		schemaID,
		name.Object(),
	).Key(params.ExecCfg().Codec)
	if err = params.p.createDescriptorWithID(
		params.ctx, key, id, desc, params.EvalContext().Settings, jobDesc,
	); err != nil {
		return err
	}

	// Initialize the sequence value.
	seqValueKey := params.ExecCfg().Codec.SequenceKey(uint32(id))
	b := &kv.Batch{}
	b.Inc(seqValueKey, desc.SequenceOpts.Start-desc.SequenceOpts.Increment)
	if err := params.p.txn.Run(params.ctx, b); err != nil {
		return err
	}

	dg := catalogkv.NewOneLevelUncachedDescGetter(params.p.txn, params.ExecCfg().Codec)
	if err := desc.Validate(params.ctx, dg); err != nil {
		return err
	}

	// Log Create Sequence event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateSequence,
		int32(desc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			SequenceName string
			Statement    string
			User         string
		}{name.FQString(), context, params.p.User().Normalized()},
	)
}

func (*createSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*createSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createSequenceNode) Close(context.Context)        {}

// NewSequenceTableDesc creates a sequence descriptor.
func NewSequenceTableDesc(
	ctx context.Context,
	sequenceName string,
	sequenceOptions tree.SequenceOptions,
	parentID descpb.ID,
	schemaID descpb.ID,
	id descpb.ID,
	creationTime hlc.Timestamp,
	privileges *descpb.PrivilegeDescriptor,
	persistence tree.Persistence,
	params *runParams,
) (*tabledesc.Mutable, error) {
	desc := tabledesc.InitTableDescriptor(
		id,
		parentID,
		schemaID,
		sequenceName,
		creationTime,
		privileges,
		persistence,
	)

	// Mimic a table with one column, "value".
	desc.Columns = []descpb.ColumnDescriptor{
		{
			ID:   tabledesc.SequenceColumnID,
			Name: tabledesc.SequenceColumnName,
			Type: types.Int,
		},
	}
	desc.PrimaryIndex = descpb.IndexDescriptor{
		ID:               keys.SequenceIndexID,
		Name:             tabledesc.PrimaryKeyIndexName,
		ColumnIDs:        []descpb.ColumnID{tabledesc.SequenceColumnID},
		ColumnNames:      []string{tabledesc.SequenceColumnName},
		ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
	}
	desc.Families = []descpb.ColumnFamilyDescriptor{
		{
			ID:              keys.SequenceColumnFamilyID,
			ColumnIDs:       []descpb.ColumnID{tabledesc.SequenceColumnID},
			ColumnNames:     []string{tabledesc.SequenceColumnName},
			Name:            "primary",
			DefaultColumnID: tabledesc.SequenceColumnID,
		},
	}

	// Fill in options, starting with defaults then overriding.
	opts := &descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	err := assignSequenceOptions(opts, sequenceOptions, true /* setDefaults */, params, id, parentID)
	if err != nil {
		return nil, err
	}
	desc.SequenceOpts = opts

	// A sequence doesn't have dependencies and thus can be made public
	// immediately.
	desc.State = descpb.DescriptorState_PUBLIC

	if err := desc.ValidateTable(ctx); err != nil {
		return nil, err
	}
	return &desc, nil
}
