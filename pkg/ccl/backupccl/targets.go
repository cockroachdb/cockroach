// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type descriptorsMatched struct {
	// all tables that match targets plus their parent databases.
	descs []sqlbase.Descriptor

	// the databases from which all tables were matched (eg a.* or DATABASE a).
	expandedDB []sqlbase.ID

	// explicitly requested DBs (e.g. DATABASE a).
	requestedDBs []*sqlbase.DatabaseDescriptor
}

func (d descriptorsMatched) checkExpansions(coveredDBs []sqlbase.ID) error {
	covered := make(map[sqlbase.ID]bool)
	for _, i := range coveredDBs {
		covered[i] = true
	}
	for _, i := range d.requestedDBs {
		if !covered[i.ID] {
			return errors.Errorf("cannot RESTORE DATABASE from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	for _, i := range d.expandedDB {
		if !covered[i] {
			return errors.Errorf("cannot RESTORE <database>.* from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	return nil
}

// descriptorResolver is the helper struct that enables reuse of the
// standard name resolution algorithm.
type descriptorResolver struct {
	descByID map[sqlbase.ID]sqlbase.Descriptor
	// Map: db name -> dbID
	dbsByName map[string]sqlbase.ID
	// Map: dbID -> obj name -> obj ID
	objsByName map[sqlbase.ID]map[string]sqlbase.ID
}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (r *descriptorResolver) LookupSchema(
	_ context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	if scName != tree.PublicSchema {
		return false, nil, nil
	}
	if dbID, ok := r.dbsByName[dbName]; ok {
		return true, r.descByID[dbID], nil
	}
	return false, nil, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (r *descriptorResolver) LookupObject(
	_ context.Context, dbName, scName, obName string,
) (bool, tree.NameResolutionResult, error) {
	if scName != tree.PublicSchema {
		return false, nil, nil
	}
	dbID, ok := r.dbsByName[dbName]
	if !ok {
		return false, nil, nil
	}
	if objMap, ok := r.objsByName[dbID]; ok {
		if objID, ok := objMap[obName]; ok {
			return true, r.descByID[objID], nil
		}
	}
	return false, nil, nil
}

// newDescriptorResolver prepares a descriptorResolver for the given
// known set of descriptors.
func newDescriptorResolver(descs []sqlbase.Descriptor) (*descriptorResolver, error) {
	r := &descriptorResolver{
		descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
		dbsByName:  make(map[string]sqlbase.ID),
		objsByName: make(map[sqlbase.ID]map[string]sqlbase.ID),
	}

	// Iterate to find the databases first. We need that because we also
	// check the ParentID for tables, and all the valid parents must be
	// known before we start to check that.
	for _, desc := range descs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if _, ok := r.dbsByName[dbDesc.Name]; ok {
				return nil, errors.Errorf("duplicate database name: %q used for ID %d and %d",
					dbDesc.Name, r.dbsByName[dbDesc.Name], dbDesc.ID)
			}
			r.dbsByName[dbDesc.Name] = dbDesc.ID
		}

		// Incidentally, also remember all the descriptors by ID.
		if prevDesc, ok := r.descByID[desc.GetID()]; ok {
			return nil, errors.Errorf("duplicate descriptor ID: %d used by %q and %q",
				desc.GetID(), prevDesc.GetName(), desc.GetName())
		}
		r.descByID[desc.GetID()] = desc
	}
	// Now on to the tables.
	for _, desc := range descs {
		if tbDesc := desc.GetTable(); tbDesc != nil {
			if tbDesc.Dropped() {
				continue
			}
			parentDesc, ok := r.descByID[tbDesc.ParentID]
			if !ok {
				return nil, errors.Errorf("table %q has unknown ParentID %d", tbDesc.Name, tbDesc.ParentID)
			}
			if _, ok := r.dbsByName[parentDesc.GetName()]; !ok {
				return nil, errors.Errorf("table %q's ParentID %d (%q) is not a database",
					tbDesc.Name, tbDesc.ParentID, parentDesc.GetName())
			}
			objMap := r.objsByName[parentDesc.GetID()]
			if objMap == nil {
				objMap = make(map[string]sqlbase.ID)
			}
			if _, ok := objMap[tbDesc.Name]; ok {
				return nil, errors.Errorf("duplicate table name: %q.%q used for ID %d and %d",
					parentDesc.GetName(), tbDesc.Name, tbDesc.ID, objMap[tbDesc.Name])
			}
			objMap[tbDesc.Name] = tbDesc.ID
			r.objsByName[parentDesc.GetID()] = objMap
		}
	}

	return r, nil
}

// descriptorsMatchingTargets returns the descriptors that match the targets. A
// database descriptor is included in this set if it matches the targets (or the
// session database) or if one of its tables matches the targets. All expanded
// DBs, via either `foo.*` or `DATABASE foo` are noted, as are those explicitly
// named as DBs (e.g. with `DATABASE foo`, not `foo.*`). These distinctions are
// used e.g. by RESTORE.
//
// This is guaranteed to not return duplicates.
func descriptorsMatchingTargets(
	ctx context.Context,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
	descriptors []sqlbase.Descriptor,
	targets tree.TargetList,
) (descriptorsMatched, error) {
	// TODO(dan): once CockroachDB supports schemas in addition to
	// catalogs, then this method will need to support it.

	ret := descriptorsMatched{}

	resolver, err := newDescriptorResolver(descriptors)
	if err != nil {
		return ret, err
	}

	alreadyRequestedDBs := make(map[sqlbase.ID]struct{})
	alreadyExpandedDBs := make(map[sqlbase.ID]struct{})
	// Process all the DATABASE requests.
	for _, d := range targets.Databases {
		dbID, ok := resolver.dbsByName[string(d)]
		if !ok {
			return ret, errors.Errorf("unknown database %q", d)
		}
		if _, ok := alreadyRequestedDBs[dbID]; !ok {
			desc := resolver.descByID[dbID]
			ret.descs = append(ret.descs, desc)
			ret.requestedDBs = append(ret.requestedDBs, desc.GetDatabase())
			ret.expandedDB = append(ret.expandedDB, dbID)
			alreadyRequestedDBs[dbID] = struct{}{}
			alreadyExpandedDBs[dbID] = struct{}{}
		}
	}

	// Process all the TABLE requests.
	// Pulling in a table needs to pull in the underlying database too.
	alreadyRequestedTables := make(map[sqlbase.ID]struct{})
	for _, pattern := range targets.Tables {
		var err error
		pattern, err = pattern.NormalizeTablePattern()
		if err != nil {
			return ret, err
		}

		switch p := pattern.(type) {
		case *tree.TableName:
			found, descI, err := p.ResolveExisting(ctx, resolver, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, errors.Errorf(`table %q does not exist`, tree.ErrString(p))
			}
			desc := descI.(sqlbase.Descriptor)

			// If the parent database is not requested already, request it now
			parentID := desc.GetTable().GetParentID()
			if _, ok := alreadyRequestedDBs[parentID]; !ok {
				parentDesc := resolver.descByID[parentID]
				ret.descs = append(ret.descs, parentDesc)
				alreadyRequestedDBs[parentID] = struct{}{}
			}
			// Then request the table itself.
			if _, ok := alreadyRequestedTables[desc.GetID()]; !ok {
				alreadyRequestedTables[desc.GetID()] = struct{}{}
				ret.descs = append(ret.descs, desc)
			}

		case *tree.AllTablesSelector:
			found, descI, err := p.TableNamePrefix.Resolve(ctx, resolver, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, sqlbase.NewInvalidWildcardError(tree.ErrString(p))
			}
			desc := descI.(sqlbase.Descriptor)

			// If the database is not requested already, request it now.
			dbID := desc.GetID()
			if _, ok := alreadyRequestedDBs[dbID]; !ok {
				ret.descs = append(ret.descs, desc)
				alreadyRequestedDBs[dbID] = struct{}{}
			}

			// Then request the expansion.
			if _, ok := alreadyExpandedDBs[desc.GetID()]; !ok {
				ret.expandedDB = append(ret.expandedDB, desc.GetID())
				alreadyExpandedDBs[desc.GetID()] = struct{}{}
			}

		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	// Then process the database expansions.
	for dbID := range alreadyExpandedDBs {
		for _, tblID := range resolver.objsByName[dbID] {
			if _, ok := alreadyRequestedTables[tblID]; !ok {
				ret.descs = append(ret.descs, resolver.descByID[tblID])
			}
		}
	}

	return ret, nil
}
