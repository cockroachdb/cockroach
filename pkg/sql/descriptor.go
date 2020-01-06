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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

// DefaultUserDBs is a set of the databases which are present in a new cluster.
var DefaultUserDBs = map[string]struct{}{
	sessiondata.DefaultDatabaseName: {},
	sessiondata.PgDatabaseName:      {},
}

// MaxDefaultDescriptorID is the maximum ID of a descriptor that exists in a
// new cluster.
var MaxDefaultDescriptorID = keys.MaxReservedDescID + sqlbase.ID(len(DefaultUserDBs))

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
	shouldCreatePublicSchema := true
	dKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, desc.Name)
	// TODO(solon): This conditional can be removed in 20.2. Every database
	// is created with a public schema for cluster version >= 20.1, so we can remove
	// the `shouldCreatePublicSchema` logic as well.
	if !cluster.Version.IsActive(ctx, p.ExecCfg().Settings, cluster.VersionNamespaceTableWithSchemas) {
		shouldCreatePublicSchema = false
	}

	if exists, _, err := sqlbase.LookupDatabaseID(ctx, p.txn, desc.Name); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, sqlbase.NewDatabaseAlreadyExistsError(desc.Name)
	} else if err != nil {
		return false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return false, err
	}

	if err := p.createDescriptorWithID(ctx, dKey.Key(), id, desc, nil); err != nil {
		return true, err
	}

	// TODO(solon): This check should be removed and a public schema should
	// be created in every database in >= 20.2.
	if shouldCreatePublicSchema {
		// Every database must be initialized with the public schema.
		if err := p.createSchemaWithID(ctx, sqlbase.NewPublicSchemaKey(id).Key(), keys.PublicSchemaID); err != nil {
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

	b := &client.Batch{}
	descID := descriptor.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
	}
	b.CPut(idKey, descID, nil)
	if err := WriteNewDescToBatch(ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), st, b, descID, descriptor); err != nil {
		return err
	}

	mutDesc, isTable := descriptor.(*sqlbase.MutableTableDescriptor)
	if isTable {
		if err := mutDesc.ValidateTable(); err != nil {
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

// resolveSchemaID resolves a schema's ID based on db and name.
func resolveSchemaID(
	ctx context.Context, txn *client.Txn, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	// Try to use the system name resolution bypass. Avoids a hotspot by explicitly
	// checking for public schema.
	if scName == tree.PublicSchema {
		return true, keys.PublicSchemaID, nil
	}

	sKey := sqlbase.NewSchemaKey(dbID, scName)
	schemaID, err := getDescriptorID(ctx, txn, sKey)
	if err != nil || schemaID == sqlbase.InvalidID {
		return false, sqlbase.InvalidID, err
	}

	return true, schemaID, nil
}

// lookupDescriptorByID looks up the descriptor for `id` and returns it.
// It can be a table or database descriptor.
// Returns the descriptor (if found), a bool representing whether the
// descriptor was found and an error if any.
func lookupDescriptorByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (sqlbase.DescriptorProto, bool, error) {
	var desc sqlbase.DescriptorProto
	for _, lookupFn := range []func() (sqlbase.DescriptorProto, error){
		func() (sqlbase.DescriptorProto, error) {
			return sqlbase.GetTableDescFromID(ctx, txn, id)
		},
		func() (sqlbase.DescriptorProto, error) {
			return sqlbase.GetDatabaseDescFromID(ctx, txn, id)
		},
	} {
		var err error
		desc, err = lookupFn()
		if err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				continue
			}
			return nil, false, err
		}
		return desc, true, nil
	}
	return nil, false, nil
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
	ts, err := txn.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return err
	}
	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.Table(ts)
		if table == nil {
			return pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table", desc.String())
		}
		if err := table.MaybeFillInDescriptor(ctx, txn); err != nil {
			return err
		}

		if err := table.Validate(ctx, txn); err != nil {
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

// IsDefaultCreatedDescriptor returns whether or not a given descriptor ID is
// present at the time of starting a cluster.
func IsDefaultCreatedDescriptor(descID sqlbase.ID) bool {
	return descID <= MaxDefaultDescriptorID
}

// CountUserDescriptors returns the number of descriptors present that were
// created by the user (i.e. not present when the cluster started).
func CountUserDescriptors(ctx context.Context, txn *client.Txn) (int, error) {
	allDescs, err := GetAllDescriptors(ctx, txn)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, desc := range allDescs {
		if !IsDefaultCreatedDescriptor(desc.GetID()) {
			count++
		}
	}

	return count, nil
}

// GetAllDescriptors looks up and returns all available descriptors.
func GetAllDescriptors(ctx context.Context, txn *client.Txn) ([]sqlbase.DescriptorProto, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := sqlbase.MakeAllDescsMetadataKey()
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	descs := make([]sqlbase.DescriptorProto, 0, len(kvs))
	for _, kv := range kvs {
		desc := &sqlbase.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Table:
			table := desc.Table(kv.Value.Timestamp)
			if err := table.MaybeFillInDescriptor(ctx, txn); err != nil {
				return nil, err
			}
			descs = append(descs, table)
		case *sqlbase.Descriptor_Database:
			descs = append(descs, desc.GetDatabase())
		default:
			return nil, errors.AssertionFailedf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}

// GetAllDatabaseDescriptorIDs looks up and returns all available database
// descriptor IDs.
func GetAllDatabaseDescriptorIDs(ctx context.Context, txn *client.Txn) ([]sqlbase.ID, error) {
	log.Eventf(ctx, "fetching all database descriptor IDs")
	nameKey := sqlbase.NewDatabaseKey("" /* name */).Key()
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /*maxRows */)
	if err != nil {
		return nil, err
	}
	// See the comment in physical_schema_accessors.go,
	// func (a UncachedPhysicalAccessor) GetObjectNames. Same concept
	// applies here.
	// TODO(solon): This complexity can be removed in 20.2.
	nameKey = sqlbase.NewDeprecatedDatabaseKey("" /* name */).Key()
	dkvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}
	kvs = append(kvs, dkvs...)

	descIDs := make([]sqlbase.ID, 0, len(kvs))
	alreadySeen := make(map[sqlbase.ID]bool)
	for _, kv := range kvs {
		ID := sqlbase.ID(kv.ValueInt())
		if alreadySeen[ID] {
			continue
		}
		alreadySeen[ID] = true
		descIDs = append(descIDs, ID)
	}
	return descIDs, nil
}

// writeDescToBatch adds a Put command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID. If kvTrace
// is enabled, it will log an event explaining the put that was performed.
func writeDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *client.Batch,
	descID sqlbase.ID,
	desc sqlbase.DescriptorProto,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(desc)
	if kvTrace {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.Put(descKey, descDesc)
	return nil
}

// WriteNewDescToBatch adds a CPut command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID, asserting
// that there was no previous descriptor at that id present already. If kvTrace
// is enabled, it will log an event explaining the CPut that was performed.
func WriteNewDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *client.Batch,
	tableID sqlbase.ID,
	desc sqlbase.DescriptorProto,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(tableID)
	descDesc := sqlbase.WrapDescriptor(desc)
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(descKey, descDesc, nil)
	return nil
}
