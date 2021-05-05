// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"gopkg.in/yaml.v2"
)

// SetZoneConfig is a partial implementation of the ALTER TABLE ... CONFIGURE
// ZONE USING statement.
func (tc *Catalog) SetZoneConfig(stmt *tree.SetZoneConfig) *zonepb.ZoneConfig {
	// Update the table name to include catalog and schema if not provided.
	tabName := stmt.TableOrIndex.Table
	tc.qualifyTableName(&tabName)
	tab := tc.Table(&tabName)

	// Handle the case of a zone config targeting a partition.
	if stmt.TargetsPartition() {
		partitionName := string(stmt.Partition)
		var index *Index
		if stmt.TableOrIndex.Index == "" {
			// This partition is in the primary index.
			index = tab.Indexes[0]
		} else {
			// This partition is in a secondary index.
			for _, idx := range tab.Indexes {
				if idx.IdxName == string(stmt.TableOrIndex.Index) {
					index = idx
					break
				}
			}
		}
		if index == nil {
			panic(fmt.Errorf("\"%q\" is not an index", stmt.TableOrIndex.Index))
		}

		for i := range index.partitions {
			if index.partitions[i].name == partitionName {
				index.partitions[i].zone = makeZoneConfig(stmt.Options)
				return index.partitions[i].zone
			}
		}
		panic(fmt.Errorf("\"%q\" is not a partition", stmt.Partition))
	}

	// The zone config must target an entire index.

	// Handle special case of primary index.
	if stmt.TableOrIndex.Index == "" {
		tab.Indexes[0].IdxZone = makeZoneConfig(stmt.Options)
		return tab.Indexes[0].IdxZone
	}

	for _, idx := range tab.Indexes {
		if idx.IdxName == string(stmt.TableOrIndex.Index) {
			idx.IdxZone = makeZoneConfig(stmt.Options)
			return idx.IdxZone
		}
	}
	panic(fmt.Errorf("\"%q\" is not an index", stmt.TableOrIndex.Index))
}

// makeZoneConfig constructs a ZoneConfig from options provided to the CONFIGURE
// ZONE USING statement.
func makeZoneConfig(options tree.KVOptions) *zonepb.ZoneConfig {
	zone := &zonepb.ZoneConfig{}
	for i := range options {
		switch options[i].Key {
		case "constraints":
			constraintsList := &zonepb.ConstraintsList{}
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), constraintsList); err != nil {
				panic(err)
			}
			zone.Constraints = constraintsList.Constraints

		case "voter_constraints":
			constraintsList := &zonepb.ConstraintsList{}
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), constraintsList); err != nil {
				panic(err)
			}
			zone.VoterConstraints = constraintsList.Constraints
			zone.NullVoterConstraintsIsEmpty = true

		case "lease_preferences":
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), &zone.LeasePreferences); err != nil {
				panic(err)
			}
		}
	}
	return zone
}
