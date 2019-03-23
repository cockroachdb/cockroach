// Copyright 2019 The Cockroach Authors.
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

package testcat

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"gopkg.in/yaml.v2"
)

// SetZoneConfig is a partial implementation of the ALTER TABLE ... CONFIGURE
// ZONE USING statement.
func (tc *Catalog) SetZoneConfig(stmt *tree.SetZoneConfig) *config.ZoneConfig {
	// Update the table name to include catalog and schema if not provided.
	tabName := stmt.TableOrIndex.Table
	tc.qualifyTableName(&tabName)
	tab := tc.Table(&tabName)

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
func makeZoneConfig(options tree.KVOptions) *config.ZoneConfig {
	zone := &config.ZoneConfig{}
	for i := range options {
		switch options[i].Key {
		case "constraints":
			constraintsList := &config.ConstraintsList{}
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), constraintsList); err != nil {
				panic(err)
			}
			zone.Constraints = constraintsList.Constraints

		case "lease_preferences":
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), &zone.LeasePreferences); err != nil {
				panic(err)
			}
		}
	}
	return zone
}
