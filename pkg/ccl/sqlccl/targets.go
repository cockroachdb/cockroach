// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

// descriptorsMatchingTargets returns the descriptors that match the targets. A
// database descriptor is included in this set if it matches the targets (or the
// session database) or if one of its tables matches the targets. All expanded
// DBs, via either `foo.*` or `DATABASE foo` are noted, as are those explicitly
// named as DBs (e.g. with `DATABASE foo`, not `foo.*`). These distinctions are
// used e.g. by RESTORE.
func descriptorsMatchingTargets(
	sessionDatabase string, descriptors []sqlbase.Descriptor, targets tree.TargetList,
) (descriptorsMatched, error) {
	// TODO(dan): If the session search path starts including more than virtual
	// tables (as of 2017-01-12 it's only pg_catalog), then this method will
	// need to support it.

	ret := descriptorsMatched{}

	type validity int
	const (
		maybeValid validity = iota
		valid
	)

	explicitlyNamedDBs := map[string]struct{}{}

	starByDatabase := make(map[string]validity, len(targets.Databases))
	for _, d := range targets.Databases {
		explicitlyNamedDBs[string(d)] = struct{}{}
		starByDatabase[string(d)] = maybeValid
	}

	type table struct {
		name     string
		validity validity
	}

	tablesByDatabase := make(map[string][]table, len(targets.Tables))
	for _, pattern := range targets.Tables {
		var err error
		pattern, err = pattern.NormalizeTablePattern()
		if err != nil {
			return ret, err
		}

		switch p := pattern.(type) {
		case *tree.TableName:
			if sessionDatabase != "" {
				if err := p.QualifyWithDatabase(sessionDatabase); err != nil {
					return ret, err
				}
			}
			db := string(p.DatabaseName)
			tablesByDatabase[db] = append(tablesByDatabase[db], table{
				name:     string(p.TableName),
				validity: maybeValid,
			})
		case *tree.AllTablesSelector:
			if sessionDatabase != "" {
				if err := p.QualifyWithDatabase(sessionDatabase); err != nil {
					return ret, err
				}
			}
			starByDatabase[string(p.Database)] = maybeValid
		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor, len(descriptors))

	for _, desc := range descriptors {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			databasesByID[dbDesc.ID] = dbDesc
			normalizedDBName := dbDesc.Name
			if _, ok := explicitlyNamedDBs[normalizedDBName]; ok {
				ret.requestedDBs = append(ret.requestedDBs, dbDesc)
			}
			if _, ok := starByDatabase[normalizedDBName]; ok {
				starByDatabase[normalizedDBName] = valid
				ret.expandedDB = append(ret.expandedDB, dbDesc.ID)
				ret.descs = append(ret.descs, desc)
			} else if _, ok := tablesByDatabase[normalizedDBName]; ok {
				ret.descs = append(ret.descs, desc)
			}
		}
	}

	for _, desc := range descriptors {
		if tableDesc := desc.GetTable(); tableDesc != nil {
			if tableDesc.Dropped() {
				continue
			}
			dbDesc, ok := databasesByID[tableDesc.ParentID]
			if !ok {
				return ret, errors.Errorf("unknown ParentID: %d", tableDesc.ParentID)
			}
			normalizedDBName := dbDesc.Name
			if tables, ok := tablesByDatabase[normalizedDBName]; ok {
				for i := range tables {
					if tables[i].name == tableDesc.Name {
						tables[i].validity = valid
						ret.descs = append(ret.descs, desc)
						break
					}
				}
			} else if _, ok := starByDatabase[normalizedDBName]; ok {
				ret.descs = append(ret.descs, desc)
			}
		}
	}

	for dbName, validity := range starByDatabase {
		if validity != valid {
			if dbName == "" {
				return ret, errors.Errorf("no database specified for wildcard")
			}
			return ret, errors.Errorf(`database "%s" does not exist`, dbName)
		}
	}

	// explicitlyNamedDBs is a subset of starByDatabase, so no need to verify it.

	for _, tables := range tablesByDatabase {
		for _, table := range tables {
			if table.validity != valid {
				return ret, errors.Errorf(`table "%s" does not exist`, table.name)
			}
		}
	}

	return ret, nil
}
