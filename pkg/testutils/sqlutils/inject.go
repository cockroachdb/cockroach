// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// InjectDescriptors attempts to inject the provided descriptors into the
// database.
// If force is true, we can inject descriptors that are invalid.
func InjectDescriptors(
	ctx context.Context, db *gosql.DB, input []*descpb.Descriptor, force bool,
) error {
	cloneInput := func() []*descpb.Descriptor {
		cloned := make([]*descpb.Descriptor, 0, len(input))
		for _, d := range input {
			cloned = append(cloned, protoutil.Clone(d).(*descpb.Descriptor))
		}
		return cloned
	}
	findDatabases := func(descs []*descpb.Descriptor) (dbs, others []*descpb.Descriptor) {
		for _, d := range descs {
			if _, ok := d.Union.(*descpb.Descriptor_Database); ok {
				dbs = append(dbs, d)
			} else {
				others = append(others, d)
			}
		}
		return dbs, others
	}
	injectDescriptor := func(tx *gosql.Tx, id descpb.ID, desc *descpb.Descriptor) error {
		resetVersionAndModificationTime(desc)
		encoded, err := protoutil.Marshal(desc)
		if err != nil {
			return err
		}
		_, err = tx.Exec(
			"SELECT crdb_internal.unsafe_upsert_descriptor($1, $2, $3)",
			id, encoded, force,
		)
		return err
	}
	injectNamespaceEntry := func(
		tx *gosql.Tx, parent, schema descpb.ID, name string, id descpb.ID,
	) error {
		_, err := tx.Exec(
			"SELECT crdb_internal.unsafe_upsert_namespace_entry($1, $2, $3, $4)",
			parent, schema, name, id,
		)
		return err
	}
	return crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
		descriptors := cloneInput()

		// Pick out the databases and inject them.
		databases, others := findDatabases(descriptors)
		for _, db := range databases {
			id, _, name, _, _, err := descpb.GetDescriptorMetadata(db)
			if err != nil {
				return err
			}
			if err := injectDescriptor(tx, id, db); err != nil {
				return errors.Wrapf(err, "failed to inject database descriptor %d", id)
			}
			if err := injectNamespaceEntry(tx, 0, 0, name, id); err != nil {
				return errors.Wrapf(err, "failed to inject namespace entry for database %d", id)
			}
			if err := injectNamespaceEntry(
				tx, id, 0, tree.PublicSchema, keys.PublicSchemaID,
			); err != nil {
				return errors.Wrapf(err, "failed to inject namespace entry for public schema in %d", id)
			}
		}
		// Inject the other descriptors - this won't do much in the way of
		// validation.
		for _, d := range others {
			id, _, _, _, _, err := descpb.GetDescriptorMetadata(d)
			if err != nil {
				return err
			}
			if err := injectDescriptor(tx, id, d); err != nil {
				return errors.Wrapf(err, "failed to inject descriptor %d", id)
			}
		}
		// Inject the namespace entries.
		for _, d := range others {
			id, _, name, _, _, err := descpb.GetDescriptorMetadata(d)
			if err != nil {
				return err
			}
			parent, schema := getDescriptorParentAndSchema(d)
			if err := injectNamespaceEntry(tx, parent, schema, name, id); err != nil {
				return errors.Wrapf(err, "failed to inject namespace entry (%d, %d, %s) %d",
					parent, schema, name, id)
			}
		}
		return nil
	})
}

func getDescriptorParentAndSchema(d *descpb.Descriptor) (parent, schema descpb.ID) {
	switch d := d.Union.(type) {
	case *descpb.Descriptor_Database:
		return 0, 0
	case *descpb.Descriptor_Schema:
		return d.Schema.ParentID, 0
	case *descpb.Descriptor_Type:
		return d.Type.ParentID, d.Type.ParentSchemaID
	case *descpb.Descriptor_Table:
		schema := d.Table.UnexposedParentSchemaID
		// Descriptors from prior to 20.1 carry a 0 schema ID.
		if schema == 0 {
			schema = keys.PublicSchemaID
		}
		return d.Table.ParentID, schema
	default:
		panic(errors.Errorf("unknown descriptor type %T", d))
	}
}

func resetVersionAndModificationTime(d *descpb.Descriptor) {
	switch d := d.Union.(type) {
	case *descpb.Descriptor_Database:
		d.Database.Version = 1
	case *descpb.Descriptor_Schema:
		d.Schema.Version = 1
	case *descpb.Descriptor_Type:
		d.Type.Version = 1
	case *descpb.Descriptor_Table:
		d.Table.Version = 1
	}
}
