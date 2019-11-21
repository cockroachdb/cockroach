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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type createSequenceNode struct {
	n      *tree.CreateSequence
	dbDesc *sqlbase.ImmutableDatabaseDescriptor
}

func (p *planner) CreateSequence(ctx context.Context, n *tree.CreateSequence) (planNode, error) {
	un := n.Name.ToUnresolvedObjectName()
	dbDesc, prefix, err := p.ResolveUncachedDatabase(ctx, un)
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
	isTemporary := n.n.Temporary

	_, schemaID, err := getTableCreateParams(params, n.dbDesc.GetID(), isTemporary, n.n.Name.Table())
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}

	return doCreateSequence(
		params, n.n.String(), n.dbDesc, schemaID, &n.n.Name, isTemporary, n.n.Options,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

// doCreateSequence performs the creation of a sequence in KV. The
// context argument is a string to use in the event log.
func doCreateSequence(
	params runParams,
	context string,
	dbDesc *sqlbase.ImmutableDatabaseDescriptor,
	schemaID sqlbase.ID,
	name *TableName,
	isTemporary bool,
	opts tree.SequenceOptions,
	jobDesc string,
) error {
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB, params.p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	// Inherit permissions from the database descriptor.
	privs := dbDesc.GetPrivileges()

	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempSequenceCounter)
	}

	// creationTime is initialized to a zero value and populated at read time.
	// See the comment in desc.MaybeIncrementVersion.
	//
	// TODO(ajwerner): remove the timestamp from MakeSequenceTableDesc, it's
	// currently relied on in import and restore code and tests.
	var creationTime hlc.Timestamp
	desc, err := MakeSequenceTableDesc(
		name.Table(),
		opts,
		dbDesc.GetID(),
		schemaID,
		id,
		creationTime,
		privs,
		isTemporary,
		&params,
	)
	if err != nil {
		return err
	}

	// makeSequenceTableDesc already validates the table. No call to
	// desc.ValidateTable() needed here.

	key := sqlbase.MakeObjectNameKey(
		params.ctx,
		params.ExecCfg().Settings,
		dbDesc.GetID(),
		schemaID,
		name.Table(),
	).Key(params.ExecCfg().Codec)
	if err = params.p.createDescriptorWithID(
		params.ctx, key, id, &desc, params.EvalContext().Settings, jobDesc,
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

	if err := desc.Validate(params.ctx, params.p.txn, params.ExecCfg().Codec); err != nil {
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
		}{name.FQString(), context, params.SessionData().User},
	)
}

func (*createSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*createSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createSequenceNode) Close(context.Context)        {}

// MakeSequenceTableDesc creates a sequence descriptor.
func MakeSequenceTableDesc(
	sequenceName string,
	sequenceOptions tree.SequenceOptions,
	parentID sqlbase.ID,
	schemaID sqlbase.ID,
	id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	isTemporary bool,
	params *runParams,
) (sqlbase.MutableTableDescriptor, error) {
	desc := sqlbase.InitTableDescriptor(
		id,
		parentID,
		schemaID,
		sequenceName,
		creationTime,
		privileges,
		isTemporary,
	)

	// Mimic a table with one column, "value".
	desc.Columns = []sqlbase.ColumnDescriptor{
		{
			ID:   sqlbase.SequenceColumnID,
			Name: sqlbase.SequenceColumnName,
			Type: types.Int,
		},
	}
	desc.PrimaryIndex = sqlbase.IndexDescriptor{
		ID:               keys.SequenceIndexID,
		Name:             sqlbase.PrimaryKeyIndexName,
		ColumnIDs:        []sqlbase.ColumnID{sqlbase.SequenceColumnID},
		ColumnNames:      []string{sqlbase.SequenceColumnName},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
	}
	desc.Families = []sqlbase.ColumnFamilyDescriptor{
		{
			ID:              keys.SequenceColumnFamilyID,
			ColumnIDs:       []sqlbase.ColumnID{sqlbase.SequenceColumnID},
			ColumnNames:     []string{sqlbase.SequenceColumnName},
			Name:            "primary",
			DefaultColumnID: sqlbase.SequenceColumnID,
		},
	}

	// Fill in options, starting with defaults then overriding.
	opts := &sqlbase.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	err := assignSequenceOptions(opts, sequenceOptions, true /* setDefaults */, params, id)
	if err != nil {
		return desc, err
	}
	desc.SequenceOpts = opts

	// A sequence doesn't have dependencies and thus can be made public
	// immediately.
	desc.State = sqlbase.TableDescriptor_PUBLIC

	return desc, desc.ValidateTable()
}
