// Copyright 2016 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/descid"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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
	errEmptyDatabaseName = pgerror.NewError(pgerror.CodeSyntaxError, "empty database name")
	errNoDatabase        = pgerror.NewError(pgerror.CodeInvalidNameError, "no database specified")
	errNoTable           = pgerror.NewError(pgerror.CodeInvalidNameError, "no table specified")
	errNoMatch           = pgerror.NewError(pgerror.CodeUndefinedObjectError, "no object matched")
)

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueDescID(ctx context.Context, db *client.DB) (descid.T, error) {
	// Increment unique descriptor counter.
	newVal, err := client.IncrementValRetryable(ctx, db, keys.DescIDGenerator, 1)
	if err != nil {
		return descid.InvalidID, err
	}
	return descid.T(newVal - 1), nil
}

// createdatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, desc *catpb.DatabaseDescriptor, ifNotExists bool,
) (bool, error) {
	plainKey := databaseKey{desc.Name}
	idKey := plainKey.Key()

	if exists, err := descExists(ctx, p.txn, idKey); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, sqlerrors.NewDatabaseAlreadyExistsError(plainKey.Name())
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
	id descid.T,
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
		if err := sqlbase.ValidateSingleTable(mutDesc.TableDesc(), st); err != nil {
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
) (descid.T, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return descid.InvalidID, err
	}
	if !gr.Exists() {
		return descid.InvalidID, nil
	}
	return descid.T(gr.ValueInt()), nil
}

// getDescriptorByID looks up the descriptor for `id`, validates it,
// and unmarshals it into `descriptor`.
//
// In most cases you'll want to use wrappers: `getDatabaseDescByID` or
// `getTableDescByID`.
func getDescriptorByID(
	ctx context.Context, txn *client.Txn, id descid.T, descriptor sqlbase.DescriptorProto,
) error {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := sqlbase.MakeDescMetadataKey(id)
	desc := &catpb.Descriptor{}
	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return err
	}

	switch t := descriptor.(type) {
	case *catpb.TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
				"%q is not a table", desc.String())
		}
		table.MaybeFillInDescriptor()

		if err := ValidateTableDescriptor(ctx, table, txn, nil /* clusterVersion */); err != nil {
			return err
		}
		*t = *table
	case *catpb.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
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
		desc := &catpb.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *catpb.Descriptor_Table:
			descs[i] = desc.GetTable()
		case *catpb.Descriptor_Database:
			descs[i] = desc.GetDatabase()
		default:
			return nil, pgerror.NewAssertionErrorf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}

// ValidateTableDescriptor validates that the table descriptor is well formed.
// Checks include both single table and cross table invariants.
func ValidateTableDescriptor(
	ctx context.Context, desc *catpb.TableDescriptor, txn *client.Txn, st *cluster.Settings,
) error {
	if err := sqlbase.ValidateSingleTable(desc, st); err != nil {
		return err
	}
	if desc.Dropped() {
		return nil
	}
	return validateCrossReferences(ctx, desc, txn)
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func validateCrossReferences(
	ctx context.Context, desc *catpb.TableDescriptor, txn *client.Txn,
) error {
	// Check that parent DB exists.
	{
		res, err := txn.Get(ctx, sqlbase.MakeDescMetadataKey(desc.ParentID))
		if err != nil {
			return err
		}
		if !res.Exists() {
			return pgerror.NewAssertionErrorf("parentID %d does not exist", log.Safe(desc.ParentID))
		}
	}

	tablesByID := map[descid.T]*TableDescriptor{desc.ID: desc}
	getTable := func(id descid.T) (*TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := sqlbase.GetTableDescFromID(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		tablesByID[id] = table
		return table, nil
	}

	findTargetIndex := func(tableID descid.T, indexID catpb.IndexID) (*TableDescriptor, *catpb.IndexDescriptor,
		error) {
		targetTable, err := getTable(tableID)
		if err != nil {
			return nil, nil, pgerror.NewAssertionErrorWithWrappedErrf(err,
				"missing table=%d index=%d", log.Safe(tableID), log.Safe(indexID))
		}
		targetIndex, err := targetTable.FindIndexByID(indexID)
		if err != nil {
			return nil, nil, pgerror.NewAssertionErrorWithWrappedErrf(err,
				"missing table=%s index=%d", targetTable.Name, log.Safe(indexID))
		}
		return targetTable, targetIndex, nil
	}

	for _, index := range desc.AllNonDropIndexes() {
		// Check foreign keys.
		if index.ForeignKey.IsSet() {
			targetTable, targetIndex, err := findTargetIndex(
				index.ForeignKey.Table, index.ForeignKey.Index)
			if err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err, "invalid foreign key")
			}
			found := false
			for _, backref := range targetIndex.ReferencedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return pgerror.NewAssertionErrorf("missing fk back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		fkBackrefs := make(map[catpb.ForeignKeyReference]struct{})
		for _, backref := range index.ReferencedBy {
			if _, ok := fkBackrefs[backref]; ok {
				return pgerror.NewAssertionErrorf("duplicated fk backreference %+v", backref)
			}
			fkBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err, "invalid fk backreference table=%d index=%d",
					backref.Table, log.Safe(backref.Index))
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err, "invalid fk backreference table=%s index=%d",
					targetTable.Name, log.Safe(backref.Index))
			}
			if fk := targetIndex.ForeignKey; fk.Table != desc.ID || fk.Index != index.ID {
				return pgerror.NewAssertionErrorf("broken fk backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}

		// Check interleaves.
		if len(index.Interleave.Ancestors) > 0 {
			// Only check the most recent ancestor, the rest of them don't point
			// back.
			ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
			targetTable, targetIndex, err := findTargetIndex(ancestor.TableID, ancestor.IndexID)
			if err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err, "invalid interleave")
			}
			found := false
			for _, backref := range targetIndex.InterleavedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return pgerror.NewAssertionErrorf(
					"missing interleave back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		interleaveBackrefs := make(map[catpb.ForeignKeyReference]struct{})
		for _, backref := range index.InterleavedBy {
			if _, ok := interleaveBackrefs[backref]; ok {
				return pgerror.NewAssertionErrorf("duplicated interleave backreference %+v", backref)
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err,
					"invalid interleave backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err,
					"invalid interleave backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if len(targetIndex.Interleave.Ancestors) == 0 {
				return pgerror.NewAssertionErrorf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
			// The last ancestor is required to be a backreference.
			ancestor := targetIndex.Interleave.Ancestors[len(targetIndex.Interleave.Ancestors)-1]
			if ancestor.TableID != desc.ID || ancestor.IndexID != index.ID {
				return pgerror.NewAssertionErrorf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.
	return nil
}
