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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type createSequenceNode struct {
	n      *tree.CreateSequence
	dbDesc catalog.DatabaseDescriptor
}

func (p *planner) CreateSequence(ctx context.Context, n *tree.CreateSequence) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE SEQUENCE",
	); err != nil {
		return nil, err
	}

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

	schemaDesc, err := getSchemaForCreateTable(params, n.dbDesc, n.n.Persistence, &n.n.Name,
		tree.ResolveRequireSequenceDesc, n.n.IfNotExists)
	if err != nil {
		if sqlerrors.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}

	_, err = doCreateSequence(
		params.ctx, params.p, params.SessionData(), n.dbDesc, schemaDesc, &n.n.Name, n.n.Persistence, n.n.Options,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)

	return err
}

// doCreateSequence performs the creation of a sequence in KV. The
// context argument is a string to use in the event log.
func doCreateSequence(
	ctx context.Context,
	p *planner,
	sessionData *sessiondata.SessionData,
	dbDesc catalog.DatabaseDescriptor,
	scDesc catalog.SchemaDescriptor,
	name *tree.TableName,
	persistence tree.Persistence,
	opts tree.SequenceOptions,
	jobDesc string,
) (*tabledesc.Mutable, error) {
	id, err := descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return nil, err
	}

	privs := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		dbDesc.GetDefaultPrivilegeDescriptor(),
		scDesc.GetDefaultPrivilegeDescriptor(),
		dbDesc.GetID(),
		sessionData.User(),
		tree.Sequences,
		dbDesc.GetPrivileges(),
	)

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
		ctx,
		p,
		p.EvalContext().Settings,
		name.Object(),
		opts,
		dbDesc.GetID(),
		scDesc.GetID(),
		id,
		creationTime,
		privs,
		persistence,
		dbDesc.IsMultiRegion(),
	)
	if err != nil {
		return nil, err
	}

	// makeSequenceTableDesc already validates the table. No call to
	// desc.ValidateSelf() needed here.

	key := catalogkeys.MakeObjectNameKey(p.ExecCfg().Codec, dbDesc.GetID(), scDesc.GetID(), name.Object())
	if err = p.createDescriptorWithID(ctx, key, id, desc, jobDesc); err != nil {
		return nil, err
	}

	// Initialize the sequence value.
	seqValueKey := p.ExecCfg().Codec.SequenceKey(uint32(id))
	b := &kv.Batch{}
	b.Inc(seqValueKey, desc.SequenceOpts.Start-desc.SequenceOpts.Increment)
	if err := p.txn.Run(ctx, b); err != nil {
		return nil, err
	}

	if err := validateDescriptor(ctx, p, desc); err != nil {
		return nil, err
	}

	// Log Create Sequence event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return desc, p.logEvent(ctx,
		desc.ID,
		&eventpb.CreateSequence{
			SequenceName: name.FQString(),
		})
}

func createSequencesForSerialColumns(
	ctx context.Context,
	p *planner,
	sessionData *sessiondata.SessionData,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	n *tree.CreateTable,
) (map[tree.Name]*tabledesc.Mutable, error) {
	colNameToSeqDesc := make(map[tree.Name]*tabledesc.Mutable)
	createStmt := n
	ensureCopy := func() {
		if createStmt == n {
			newCreateStmt := *n
			n.Defs = append(tree.TableDefs(nil), n.Defs...)
			createStmt = &newCreateStmt
		}
	}

	tn := tree.MakeTableNameFromPrefix(catalog.ResolvedObjectPrefix{
		Database: db,
		Schema:   sc,
	}.NamePrefix(), tree.Name(n.Table.Table()))
	for i, def := range n.Defs {
		d, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		newDef, prefix, seqName, seqOpts, err := p.processSerialLikeInColumnDef(ctx, d, &tn)
		if err != nil {
			return nil, err
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if seqName != nil {
			seqDesc, err := doCreateSequence(
				ctx,
				p,
				sessionData,
				prefix.Database,
				prefix.Schema,
				seqName,
				n.Persistence,
				seqOpts,
				fmt.Sprintf("creating sequence %s for new table %s", seqName, n.Table.Table()),
			)
			if err != nil {
				return nil, err
			}
			colNameToSeqDesc[d.Name] = seqDesc
		}
		if d != newDef {
			ensureCopy()
			n.Defs[i] = newDef
		}
	}

	return colNameToSeqDesc, nil
}

func (*createSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*createSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createSequenceNode) Close(context.Context)        {}

// NewSequenceTableDesc creates a sequence descriptor.
func NewSequenceTableDesc(
	ctx context.Context,
	p *planner,
	settings *clustersettings.Settings,
	sequenceName string,
	sequenceOptions tree.SequenceOptions,
	parentID descpb.ID,
	schemaID descpb.ID,
	id descpb.ID,
	creationTime hlc.Timestamp,
	privileges *catpb.PrivilegeDescriptor,
	persistence tree.Persistence,
	isMultiRegion bool,
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
	desc.SetPrimaryIndex(descpb.IndexDescriptor{
		ID:                  keys.SequenceIndexID,
		Name:                tabledesc.LegacyPrimaryKeyIndexName,
		KeyColumnIDs:        []descpb.ColumnID{tabledesc.SequenceColumnID},
		KeyColumnNames:      []string{tabledesc.SequenceColumnName},
		KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
		EncodingType:        descpb.PrimaryIndexEncoding,
		Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
		CreatedAtNanos:      creationTime.WallTime,
	})
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
	if err := assignSequenceOptions(
		ctx,
		p,
		opts,
		sequenceOptions,
		true, /* setDefaults */
		id,
		parentID,
		nil, /* existingType */
	); err != nil {
		return nil, err
	}
	desc.SequenceOpts = opts

	// A sequence doesn't have dependencies and thus can be made public
	// immediately.
	desc.State = descpb.DescriptorState_PUBLIC

	if isMultiRegion {
		desc.SetTableLocalityRegionalByTable(tree.PrimaryRegionNotSpecifiedName)
	}

	version := settings.Version.ActiveVersion(ctx)
	if err := descbuilder.ValidateSelf(&desc, version); err != nil {
		return nil, err
	}
	return &desc, nil
}
