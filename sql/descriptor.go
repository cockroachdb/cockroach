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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

var (
	errEmptyDatabaseName = errors.New("empty database name")
	errNoDatabase        = errors.New("no database specified")
	errNoTable           = errors.New("no table specified")
)

// DescriptorAccessor provides helper methods for using descriptors
// to SQL objects.
type DescriptorAccessor interface {
	// checkPrivilege verifies that p.session.User has `privilege` on `descriptor`.
	checkPrivilege(descriptor sqlbase.DescriptorProto, privilege privilege.Kind) error

	// anyPrivilege verifies that p.session.User has any privilege on `descriptor`.
	anyPrivilege(descriptor sqlbase.DescriptorProto) error

	// createDescriptor takes a Table or Database descriptor and creates it if
	// needed, incrementing the descriptor counter. Returns true if the descriptor
	// is actually created, false if it already existed, or an error if one was encountered.
	// The ifNotExists flag is used to declare if the "already existed" state should be an
	// error (false) or a no-op (true).
	createDescriptor(plainKey sqlbase.DescriptorKey, descriptor sqlbase.DescriptorProto, ifNotExists bool) (bool, error)

	// getDescriptor looks up the descriptor for `plainKey`, validates it,
	// and unmarshals it into `descriptor`.
	// If `plainKey` doesn't exist, returns false and nil error.
	// In most cases you'll want to use wrappers: `getDatabaseDesc` or
	// `getTableDesc`.
	getDescriptor(plainKey sqlbase.DescriptorKey, descriptor sqlbase.DescriptorProto) (bool, error)

	// getAllDescriptors looks up and returns all available descriptors.
	getAllDescriptors() ([]sqlbase.DescriptorProto, error)

	// getDescriptorsFromTargetList examines a TargetList and fetches the
	// appropriate descriptors.
	getDescriptorsFromTargetList(targets parser.TargetList) ([]sqlbase.DescriptorProto, error)
}

var _ DescriptorAccessor = &planner{}

// checkPrivilege implements the DescriptorAccessor interface.
func (p *planner) checkPrivilege(descriptor sqlbase.DescriptorProto, privilege privilege.Kind) error {
	if descriptor.GetPrivileges().CheckPrivilege(p.session.User, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		p.session.User, privilege, descriptor.TypeName(), descriptor.GetName())
}

// anyPrivilege implements the DescriptorAccessor interface.
func (p *planner) anyPrivilege(descriptor sqlbase.DescriptorProto) error {
	if descriptor.GetPrivileges().AnyPrivilege(p.session.User) || isVirtualDescriptor(descriptor) {
		return nil
	}
	return fmt.Errorf("user %s has no privileges on %s %s",
		p.session.User, descriptor.TypeName(), descriptor.GetName())
}

type descriptorAlreadyExistsErr struct {
	desc sqlbase.DescriptorProto
	name string
}

func (d descriptorAlreadyExistsErr) Error() string {
	return fmt.Sprintf("%s %q already exists", d.desc.TypeName(), d.name)
}

// createDescriptor implements the DescriptorAccessor interface.
func (p *planner) createDescriptor(plainKey sqlbase.DescriptorKey, descriptor sqlbase.DescriptorProto, ifNotExists bool) (bool, error) {
	idKey := plainKey.Key()
	// Check whether idKey exists.
	gr, err := p.txn.Get(idKey)
	if err != nil {
		return false, err
	}

	if gr.Exists() {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		// Key exists, but we don't want it to: error out.
		return false, descriptorAlreadyExistsErr{descriptor, plainKey.Name()}
	}

	// Increment unique descriptor counter.
	if ir, err := p.txn.Inc(keys.DescIDGenerator, 1); err == nil {
		descriptor.SetID(sqlbase.ID(ir.ValueInt() - 1))
	} else {
		return false, err
	}

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

	b := client.Batch{}
	descID := descriptor.GetID()
	descDesc := sqlbase.WrapDescriptor(descriptor)
	if log.V(2) {
		log.Infof(p.ctx(), "CPut %s -> %d", idKey, descID)
		log.Infof(p.ctx(), "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(idKey, descID, nil)
	b.CPut(descKey, descDesc, nil)

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, idKey, descID); err != nil {
			return err
		}
		return expectDescriptor(systemConfig, descKey, descDesc)
	})

	return true, p.txn.Run(&b)
}

// getDescriptor implements the DescriptorAccessor interface.
func (p *planner) getDescriptor(plainKey sqlbase.DescriptorKey, descriptor sqlbase.DescriptorProto,
) (bool, error) {
	gr, err := p.txn.Get(plainKey.Key())
	if err != nil {
		return false, err
	}
	if !gr.Exists() {
		return false, nil
	}

	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := p.txn.GetProto(descKey, desc); err != nil {
		return false, err
	}

	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.GetTable()
		table.MaybeUpgradeFormatVersion()
		// TODO(dan): Write the upgraded TableDescriptor back to kv. This will break
		// the ability to use a previous version of cockroach with the on-disk data,
		// but it's worth it to avoid having to do the upgrade every time the
		// descriptor is fetched. Our current test for this enforces compatibility
		// backward and forward, so that'll have to be extended before this is done.
		if table == nil {
			return false, errors.Errorf("%q is not a table", plainKey.Name())
		}
		if err := table.Validate(p.txn); err != nil {
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

// getAllDescriptors implements the DescriptorAccessor interface.
func (p *planner) getAllDescriptors() ([]sqlbase.DescriptorProto, error) {
	descsKey := sqlbase.MakeAllDescsMetadataKey()
	kvs, err := p.txn.Scan(descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var descs []sqlbase.DescriptorProto
	for _, kv := range kvs {
		desc := &sqlbase.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Table:
			descs = append(descs, desc.GetTable())
		case *sqlbase.Descriptor_Database:
			descs = append(descs, desc.GetDatabase())
		default:
			return nil, errors.Errorf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}

// getDescriptorsFromTargetList implements the DescriptorAccessor interface.
func (p *planner) getDescriptorsFromTargetList(targets parser.TargetList) (
	[]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.mustGetDatabaseDesc(database)
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
	for _, tableGlob := range targets.Tables {
		tables, err := p.expandTableGlob(tableGlob)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			descriptor, err := p.mustGetTableDesc(table)
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
	}
	return descs, nil
}
