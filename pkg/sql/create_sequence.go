// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type createSequenceNode struct {
	n      *tree.CreateSequence
	dbDesc *sqlbase.DatabaseDescriptor
}

func (p *planner) CreateSequence(ctx context.Context, n *tree.CreateSequence) (planNode, error) {
	name, err := n.Name.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), name.Database())
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createSequenceNode{
		n:      n,
		dbDesc: dbDesc,
	}, nil
}

func (n *createSequenceNode) startExec(params runParams) error {
	seqName := n.n.Name.TableName().Table()
	tKey := tableKey{parentID: n.dbDesc.ID, name: seqName}
	key := tKey.Key()
	if exists, err := descExists(params.ctx, params.p.txn, key); err == nil && exists {
		if n.n.IfNotExists {
			// If the sequence exists but the user specified IF NOT EXISTS, return without doing anything.
			return nil
		}
		return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
	} else if err != nil {
		return err
	}

	id, err := GenerateUniqueDescID(params.ctx, params.p.session.execCfg.DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the database descriptor.
	privs := n.dbDesc.GetPrivileges()

	desc, err := n.makeSequenceTableDesc(params, seqName, n.dbDesc.ID, id, privs)
	if err != nil {
		return err
	}

	if err = desc.ValidateTable(); err != nil {
		return err
	}

	if err = params.p.createDescriptorWithID(params.ctx, key, id, &desc); err != nil {
		return err
	}

	// Initialize the sequence value.
	seqValueKey := keys.MakeSequenceKey(uint32(id))
	opts := &roachpb.IncrementRequest_BoundsOptions{
		MinValue: math.MinInt64,
		MaxValue: math.MaxInt64,
	}
	b := &client.Batch{}
	b.IncWithOpts(seqValueKey, desc.SequenceOpts.Start-desc.SequenceOpts.Increment, opts)
	if err := params.p.txn.Run(params.ctx, b); err != nil {
		return err
	}

	if desc.Adding() {
		params.p.notifySchemaChange(&desc, sqlbase.InvalidMutationID)
	}
	if err := desc.Validate(params.ctx, params.p.txn); err != nil {
		return err
	}

	// Log Create Sequence event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateSequence,
		int32(desc.ID),
		int32(params.evalCtx.NodeID),
		struct {
			SequenceName string
			Statement    string
			User         string
		}{n.n.Name.String(), n.n.String(), params.p.session.User},
	)
}

func (*createSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*createSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createSequenceNode) Close(context.Context)        {}

func (n *createSequenceNode) makeSequenceTableDesc(
	params runParams,
	sequenceName string,
	parentID sqlbase.ID,
	id sqlbase.ID,
	privileges *sqlbase.PrivilegeDescriptor,
) (sqlbase.TableDescriptor, error) {
	desc := initTableDescriptor(id, parentID, sequenceName, params.p.txn.OrigTimestamp(), privileges)

	// Fill in options, starting with defaults then overriding.

	opts := &sqlbase.TableDescriptor_SequenceOpts{
		Cycle:     false,
		Increment: 1,
	}
	err := assignSequenceOptions(opts, n.n.Options, true /* setDefaults */)
	if err != nil {
		return desc, err
	}
	desc.SequenceOpts = opts

	return desc, desc.AllocateIDs()
}
