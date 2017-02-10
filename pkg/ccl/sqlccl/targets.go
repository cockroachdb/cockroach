// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// descriptorsMatchingTargets returns the descriptors that match the targets. A
// database descriptor is included in this set if it matches the targets (or the
// session database) or if one of its tables matches the targets.
func descriptorsMatchingTargets(
	sessionDatabase string,
	searchPath parser.SearchPath,
	descriptors []sqlbase.Descriptor,
	targets parser.TargetList,
) ([]sqlbase.Descriptor, error) {
	// TODO(dan): If the session search path starts including more than virtual
	// tables (as of 2017-01-12 it's only pg_catalog), then this method will
	// need to support it.

	starByDatabase := make(map[string]struct{}, len(targets.Databases))
	for _, d := range targets.Databases {
		starByDatabase[d.Normalize()] = struct{}{}
	}

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor, len(descriptors))
	for _, desc := range descriptors {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			databasesByID[dbDesc.ID] = dbDesc
		}
	}

	tablesByDatabase := make(map[string][]string, len(targets.Tables))
	for _, pattern := range targets.Tables {
		var err error
		pattern, err = pattern.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}

		switch p := pattern.(type) {
		case *parser.TableName:
			if p.DatabaseName == "" {
				found, err := searchAndQualifyDatabase(
					p, sessionDatabase, searchPath, databasesByID, descriptors)
				if err != nil {
					return nil, err
				}
				if !found {
					continue
				}
			}
			db := p.DatabaseName.Normalize()
			tablesByDatabase[db] = append(tablesByDatabase[db], p.TableName.Normalize())

		case *parser.AllTablesSelector:
			if sessionDatabase != "" {
				if err := p.QualifyWithDatabase(sessionDatabase); err != nil {
					return nil, err
				}
			}
			starByDatabase[p.Database.Normalize()] = struct{}{}
		default:
			return nil, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	var ret []sqlbase.Descriptor

	for _, desc := range descriptors {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			normalizedDBName := parser.ReNormalizeName(dbDesc.Name)
			if _, ok := starByDatabase[normalizedDBName]; ok {
				ret = append(ret, desc)
			} else if _, ok := tablesByDatabase[normalizedDBName]; ok {
				ret = append(ret, desc)
			}
		}
	}

	for _, desc := range descriptors {
		if tableDesc := desc.GetTable(); tableDesc != nil {
			dbDesc, ok := databasesByID[tableDesc.ParentID]
			if !ok {
				return nil, errors.Errorf("unknown ParentID: %d", tableDesc.ParentID)
			}
			normalizedDBName := parser.ReNormalizeName(dbDesc.Name)
			if _, ok := starByDatabase[normalizedDBName]; ok {
				ret = append(ret, desc)
			} else if tableNames, ok := tablesByDatabase[normalizedDBName]; ok {
				for _, tableName := range tableNames {
					if parser.ReNormalizeName(tableName) == parser.ReNormalizeName(tableDesc.Name) {
						ret = append(ret, desc)
						break
					}
				}
			}
		}
	}
	return ret, nil
}

// searchAndQualifyDatabase emulates the logic in sql.searchAndQualifyDatabase.
func searchAndQualifyDatabase(
	tn *parser.TableName,
	sessionDatabase string,
	searchPath parser.SearchPath,
	dbByID map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	descriptors []sqlbase.Descriptor,
) (bool, error) {
	t := *tn

	if sessionDatabase != "" {
		t.DatabaseName = parser.Name(sessionDatabase)

		found, err := getTableOrViewDesc(&t, dbByID, descriptors)
		if err != nil {
			return false, err
		}
		if found {
			*tn = t
			return true, nil
		}
	}

	// Not found using the current session's database, so try
	// the search path instead.
	for _, database := range searchPath {
		t.DatabaseName = parser.Name(database)
		found, err := getTableOrViewDesc(&t, dbByID, descriptors)
		if err != nil {
			return false, err
		}
		if found {
			// The table or view exists in this database, so return it.
			*tn = t
			return true, nil
		}
	}

	return false, nil
}

func getTableOrViewDesc(
	tn *parser.TableName,
	databasesByID map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	descriptors []sqlbase.Descriptor,
) (bool, error) {
	desired := tn.TableName.Normalize()
	for _, desc := range descriptors {
		if tableDesc := desc.GetTable(); tableDesc != nil &&
			parser.ReNormalizeName(tableDesc.Name) == desired {
			dbDesc, ok := databasesByID[tableDesc.ParentID]
			if !ok {
				return false, errors.Errorf("unknown ParentID: %d", tableDesc.ParentID)
			}
			if string(tn.DatabaseName) == parser.ReNormalizeName(dbDesc.Name) {
				return true, nil
			}
		}
	}
	return false, nil
}
