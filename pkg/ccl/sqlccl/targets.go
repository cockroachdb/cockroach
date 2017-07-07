// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

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
	sessionDatabase string, descriptors []sqlbase.Descriptor, targets parser.TargetList,
) ([]sqlbase.Descriptor, error) {
	// TODO(dan): If the session search path starts including more than virtual
	// tables (as of 2017-01-12 it's only pg_catalog), then this method will
	// need to support it.

	type validity int
	const (
		maybeValid validity = iota
		valid
	)

	starByDatabase := make(map[string]validity, len(targets.Databases))
	for _, d := range targets.Databases {
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
			return nil, err
		}

		switch p := pattern.(type) {
		case *parser.TableName:
			if sessionDatabase != "" {
				if err := p.QualifyWithDatabase(sessionDatabase); err != nil {
					return nil, err
				}
			}
			db := string(p.DatabaseName)
			tablesByDatabase[db] = append(tablesByDatabase[db], table{
				name:     string(p.TableName),
				validity: maybeValid,
			})
		case *parser.AllTablesSelector:
			if sessionDatabase != "" {
				if err := p.QualifyWithDatabase(sessionDatabase); err != nil {
					return nil, err
				}
			}
			starByDatabase[string(p.Database)] = maybeValid
		default:
			return nil, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor, len(descriptors))
	var ret []sqlbase.Descriptor

	for _, desc := range descriptors {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			databasesByID[dbDesc.ID] = dbDesc
			normalizedDBName := dbDesc.Name
			if _, ok := starByDatabase[normalizedDBName]; ok {
				starByDatabase[normalizedDBName] = valid
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
			normalizedDBName := dbDesc.Name
			if tables, ok := tablesByDatabase[normalizedDBName]; ok {
				for i := range tables {
					if tables[i].name == tableDesc.Name {
						tables[i].validity = valid
						ret = append(ret, desc)
						break
					}
				}
			} else if _, ok := starByDatabase[normalizedDBName]; ok {
				ret = append(ret, desc)
			}
		}
	}

	for dbName, validity := range starByDatabase {
		if validity != valid {
			if dbName == "" {
				return nil, errors.Errorf("no database specified for wildcard")
			}
			return nil, errors.Errorf(`database "%s" does not exist`, dbName)
		}
	}

	for _, tables := range tablesByDatabase {
		for _, table := range tables {
			if table.validity != valid {
				return nil, errors.Errorf(`table "%s" does not exist`, table.name)
			}
		}
	}

	return ret, nil
}
