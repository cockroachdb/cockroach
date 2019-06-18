// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueDescID(ctx context.Context, db *client.DB) (sqlbase.ID, error) {
	// Increment unique descriptor counter.
	newVal, err := client.IncrementValRetryable(ctx, db, keys.DescIDGenerator, 1)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	return sqlbase.ID(newVal - 1), nil
}

// createdatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, desc *sqlbase.DatabaseDescriptor, ifNotExists bool,
) (bool, error) {
	plainKey := sqlbase.NewDatabaseKey(desc.Name)
	idKey := plainKey.Key()

	if exists, err := descExists(ctx, p.txn, idKey); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, sqlbase.NewDatabaseAlreadyExistsError(plainKey.Name())
	} else if err != nil {
		return false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return false, err
	}

	return true, p.createDescriptorWithID(ctx, idKey, id, desc, nil)
}

func descExists(ctx context.Context, txn *client.Txn, idKey roachpb.Key) (bool, error) {
	// Check whether idKey exists.
	gr, err := txn.Get(ctx, idKey)
	if err != nil {
		return false, err
	}
	return gr.Exists(), nil
}

func (p *planner) createDescriptorWithID(
	ctx context.Context,
	idKey roachpb.Key,
	id sqlbase.ID,
	descriptor sqlbase.DescriptorProto,
	st *cluster.Settings,
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
	descKey := sqlbase.MakeDescMetadataKey(descriptor.GetID())

	b := &client.Batch{}
	descID := descriptor.GetID()
	descDesc := sqlbase.WrapDescriptor(descriptor)
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(idKey, descID, nil)
	b.CPut(descKey, descDesc, nil)

	mutDesc, isTable := descriptor.(*sqlbase.MutableTableDescriptor)
	if isTable {
		if err := mutDesc.ValidateTable(st); err != nil {
			return err
		}
		if err := p.Tables().addUncommittedTable(*mutDesc); err != nil {
			return err
		}
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	if isTable && mutDesc.Adding() {
		p.queueSchemaChange(mutDesc.TableDesc(), sqlbase.InvalidMutationID)
	}
	return nil
}

// getDescriptorID looks up the ID for plainKey.
// InvalidID is returned if the name cannot be resolved.
func getDescriptorID(
	ctx context.Context, txn *client.Txn, plainKey sqlbase.DescriptorKey,
) (sqlbase.ID, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !gr.Exists() {
		return sqlbase.InvalidID, nil
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// getDescriptorByID looks up the descriptor for `id`, validates it,
// and unmarshals it into `descriptor`.
//
// In most cases you'll want to use wrappers: `getDatabaseDescByID` or
// `getTableDescByID`.
func getDescriptorByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID, descriptor sqlbase.DescriptorProto,
) error {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := sqlbase.MakeDescMetadataKey(id)
	desc := &sqlbase.Descriptor{}
	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return err
	}

	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table", desc.String())
		}
		table.MaybeFillInDescriptor()

		if err := table.Validate(ctx, txn, nil /* clusterVersion */); err != nil {
			return err
		}
		*t = *table
	case *sqlbase.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a database", desc.String())
		}

		if err := database.Validate(); err != nil {
			return err
		}
		*t = *database
	}
	return nil
}

// GetAllDescriptors looks up and returns all available descriptors.
func GetAllDescriptors(ctx context.Context, txn *client.Txn) ([]sqlbase.DescriptorProto, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := sqlbase.MakeAllDescsMetadataKey()
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	descs := make([]sqlbase.DescriptorProto, len(kvs))
	for i, kv := range kvs {
		desc := &sqlbase.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Table:
			descs[i] = desc.GetTable()
		case *sqlbase.Descriptor_Database:
			descs[i] = desc.GetDatabase()
		default:
			return nil, errors.AssertionFailedf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}
