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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var (
	errEmptyDatabaseName = errors.New("empty database name")
	errNoDatabase        = errors.New("no database specified")
	errNoTable           = errors.New("no table specified")
)

// DescriptorAccessor provides helper methods for using descriptors
// to SQL objects.
type DescriptorAccessor interface {
	// createDescriptor takes a Table or Database descriptor and creates it if
	// needed, incrementing the descriptor counter. Returns true if the descriptor
	// is actually created, false if it already existed, or an error if one was encountered.
	// The ifNotExists flag is used to declare if the "already existed" state should be an
	// error (false) or a no-op (true).
	createDescriptor(
		ctx context.Context,
		plainKey sqlbase.DescriptorKey,
		descriptor sqlbase.DescriptorProto,
		ifNotExists bool,
	) (bool, error)
}

var _ DescriptorAccessor = &planner{}

type descriptorAlreadyExistsErr struct {
	desc sqlbase.DescriptorProto
	name string
}

func (d descriptorAlreadyExistsErr) Error() string {
	return fmt.Sprintf("%s %q already exists", d.desc.TypeName(), d.name)
}

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter.
func GenerateUniqueDescID(ctx context.Context, txn *client.Txn) (sqlbase.ID, error) {
	// Increment unique descriptor counter.
	ir, err := txn.Inc(ctx, keys.DescIDGenerator, 1)
	if err != nil {
		return 0, err
	}
	return sqlbase.ID(ir.ValueInt() - 1), nil
}

// createDescriptor implements the DescriptorAccessor interface.
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

	id, err := GenerateUniqueDescID(ctx, p.txn)
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
	if log.V(2) {
		log.Infof(ctx, "CPut %s -> %d", idKey, descID)
		log.Infof(ctx, "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(idKey, descID, nil)
	b.CPut(descKey, descDesc, nil)

	p.session.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, idKey, descID); err != nil {
			return err
		}
		return expectDescriptor(systemConfig, descKey, descDesc)
	})

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

	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return false, err
	}

	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return false, errors.Errorf("%q is not a table", plainKey.Name())
		}
		table.MaybeUpgradeFormatVersion()
		// TODO(dan): Write the upgraded TableDescriptor back to kv. This will break
		// the ability to use a previous version of cockroach with the on-disk data,
		// but it's worth it to avoid having to do the upgrade every time the
		// descriptor is fetched. Our current test for this enforces compatibility
		// backward and forward, so that'll have to be extended before this is done.
		if err := table.Validate(ctx, txn); err != nil {
			return false, err
		}
		*t = *table
	case *sqlbase.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return false, errors.Errorf("%q is not a database", plainKey.Name())
		}
		if err := database.Validate(); err != nil {
			return false, err
		}
		*t = *database
	}
	return true, nil
}

// getAllDescriptors looks up and returns all available descriptors.
func getAllDescriptors(ctx context.Context, txn *client.Txn) ([]sqlbase.DescriptorProto, error) {
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

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, db string, targets parser.TargetList,
) ([]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := MustGetDatabaseDesc(ctx, txn, vt, string(database))
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
		return descs, nil
	}

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	}
	descs := make([]sqlbase.DescriptorProto, 0, len(targets.Tables))
	for _, tableTarget := range targets.Tables {
		tableGlob, err := tableTarget.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		tables, err := expandTableGlob(ctx, txn, vt, db, tableGlob)
		if err != nil {
			return nil, err
		}
		for i := range tables {
			descriptor, err := mustGetTableOrViewDesc(ctx, txn, vt, &tables[i])
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
	}
	return descs, nil
}
