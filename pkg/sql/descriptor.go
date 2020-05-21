// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

//
// This file contains routines for low-level access to stored
// descriptors.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

var (
	errEmptyDatabaseName = pgerror.New(pgcode.Syntax, "empty database name")
	errNoDatabase        = pgerror.New(pgcode.InvalidName, "no database specified")
	errNoTable           = pgerror.New(pgcode.InvalidName, "no table specified")
	errNoMatch           = pgerror.New(pgcode.UndefinedObject, "no object matched")
)

// createdatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, desc *sqlbase.DatabaseDescriptor, ifNotExists bool, jobDesc string,
) (bool, error) {
	shouldCreatePublicSchema := true
	dKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, desc.Name)
	// TODO(solon): This conditional can be removed in 20.2. Every database
	// is created with a public schema for cluster version >= 20.1, so we can remove
	// the `shouldCreatePublicSchema` logic as well.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) {
		shouldCreatePublicSchema = false
	}

	if exists, _, err := sqlbase.LookupDatabaseID(ctx, p.txn, p.ExecCfg().Codec, desc.Name); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, sqlbase.NewDatabaseAlreadyExistsError(desc.Name)
	} else if err != nil {
		return false, err
	}

	id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return false, err
	}

	if err := p.createDescriptorWithID(ctx, dKey.Key(p.ExecCfg().Codec), id, desc, nil, jobDesc); err != nil {
		return true, err
	}

	// TODO(solon): This check should be removed and a public schema should
	// be created in every database in >= 20.2.
	if shouldCreatePublicSchema {
		// Every database must be initialized with the public schema.
		if err := p.createSchemaWithID(ctx, sqlbase.NewPublicSchemaKey(id).Key(p.ExecCfg().Codec), keys.PublicSchemaID); err != nil {
			return true, err
		}
	}

	return true, nil
}

func (p *planner) createDescriptorWithID(
	ctx context.Context,
	idKey roachpb.Key,
	id sqlbase.ID,
	descriptor sqlbase.DescriptorProto,
	st *cluster.Settings,
	jobDesc string,
) error {
	descriptor.SetID(id)
	// TODO(pmattis): The error currently returned below is likely going to be
	// difficult to interpret.
	//
	// TODO(pmattis): Need to handle if-not-exists here as well.
	//
	// TODO(pmattis): This is writing the namespace and descriptor table entries,
	// but not going through the normal INSERT logic and not performing a precise
	// mimicry. In particular, we're only writing a single key per table, while
	// perfect mimicry would involve writing a sentinel key for each row as well.

	b := &kv.Batch{}
	descID := descriptor.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
	}
	b.CPut(idKey, descID, nil)
	if err := catalogkv.WriteNewDescToBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		st,
		b,
		p.ExecCfg().Codec,
		descID,
		descriptor,
	); err != nil {
		return err
	}

	mutDesc, isTable := descriptor.(*sqlbase.MutableTableDescriptor)
	if isTable {
		if err := mutDesc.ValidateTable(); err != nil {
			return err
		}
		if err := p.Tables().AddUncommittedTable(*mutDesc); err != nil {
			return err
		}
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	if isTable && mutDesc.Adding() {
		// Queue a schema change job to eventually make the table public.
		if err := p.createOrUpdateSchemaChangeJob(
			ctx,
			mutDesc,
			jobDesc,
			sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}
	return nil
}
