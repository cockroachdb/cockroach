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
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	errEmptyDatabaseName = pgerror.NewError(pgerror.CodeSyntaxError, "empty database name")
	errNoDatabase        = pgerror.NewError(pgerror.CodeInvalidNameError, "no database specified")
	errNoTable           = pgerror.NewError(pgerror.CodeInvalidNameError, "no table specified")
	errNoMatch           = pgerror.NewError(pgerror.CodeUndefinedObjectError, "no object matched")
)

type descriptorAlreadyExistsErr struct {
	desc sqlbase.DescriptorProto
	name string
}

func (d descriptorAlreadyExistsErr) Error() string {
	return fmt.Sprintf("%s %q already exists", d.desc.TypeName(), d.name)
}

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueDescID(ctx context.Context, db *client.DB) (sqlbase.ID, error) {
	// Increment unique descriptor counter.
	newVal, err := client.IncrementValRetryable(ctx, db, keys.DescIDGenerator, 1)
	if err != nil {
		return 0, err
	}
	return sqlbase.ID(newVal - 1), nil
}

// createDescriptor takes a Table or Database descriptor and creates it if
// needed, incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
func (p *planner) createDescriptor(
	ctx context.Context,
	plainKey sqlbase.DescriptorKey,
	descriptor sqlbase.DescriptorProto,
	ifNotExists bool,
) (bool, error) {
	idKey := plainKey.Key()

	if exists, err := descExists(ctx, p.txn, idKey); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		// Key exists, but we don't want it to: error out.
		switch descriptor.TypeName() {
		case "database":
			return false, sqlbase.NewDatabaseAlreadyExistsError(plainKey.Name())
		case "table", "view":
			return false, sqlbase.NewRelationAlreadyExistsError(plainKey.Name())
		default:
			return false, descriptorAlreadyExistsErr{descriptor, plainKey.Name()}
		}
	} else if err != nil {
		return false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return false, err
	}

	return true, p.createDescriptorWithID(ctx, idKey, id, descriptor)
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
	ctx context.Context, idKey roachpb.Key, id sqlbase.ID, descriptor sqlbase.DescriptorProto,
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

	if desc, ok := descriptor.(*sqlbase.TableDescriptor); ok {
		p.Tables().addUncommittedTable(*desc)
	}

	return p.txn.Run(ctx, b)
}

// getDescriptor looks up the descriptor for `plainKey`, validates it,
// and unmarshals it into `descriptor`.
//
// If `plainKey` doesn't exist, returns false and nil error.
// In most cases you'll want to use wrappers: `getDatabaseDesc` or
// `getTableDesc`.
func getDescriptor(
	ctx context.Context,
	txn *client.Txn,
	plainKey sqlbase.DescriptorKey,
	descriptor sqlbase.DescriptorProto,
) (bool, error) {
	gr, err := txn.Get(ctx, plainKey.Key())
	if err != nil {
		return false, err
	}
	if !gr.Exists() {
		return false, nil
	}

	if err := getDescriptorByID(ctx, txn, sqlbase.ID(gr.ValueInt()), descriptor); err != nil {
		return false, err
	}
	return true, nil
}

// getDescriptorByID looks up the descriptor for `id`, validates it,
// and unmarshals it into `descriptor`.
//
// In most cases you'll want to use wrappers: `getDatabaseDescByID` or
// `getTableDescByID`.
func getDescriptorByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID, descriptor sqlbase.DescriptorProto,
) error {
	descKey := sqlbase.MakeDescMetadataKey(id)
	desc := &sqlbase.Descriptor{}
	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return err
	}

	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return errors.Errorf("%q is not a table", desc.String())
		}
		table.MaybeFillInDescriptor()

		if err := table.Validate(ctx, txn, nil /* clusterVersion */); err != nil {
			return err
		}
		*t = *table
	case *sqlbase.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return errors.Errorf("%q is not a database", desc.String())
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
			return nil, errors.Errorf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}
