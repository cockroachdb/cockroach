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
// Author: Tristan Rice (rice@fn.lc)

package config

import "github.com/cockroachdb/cockroach/roachpb"

// MigrateZoneConfig migrates the legacy ZoneConfig format into the new one.
func MigrateZoneConfig(value *roachpb.Value) (ZoneConfig, error) {
	var zone ZoneConfig
	if err := value.GetProto(&zone); err != nil {
		return ZoneConfig{}, err
	}
	var legacyZone ZoneConfigLegacy
	if err := value.GetProto(&legacyZone); err != nil {
		return ZoneConfig{}, err
	}
	if len(legacyZone.ReplicaAttrs) > 0 {
		zone.NumReplicas = int32(len(legacyZone.ReplicaAttrs))
		if zone.NumReplicas > 0 {
			var err error
			zone.Constraints, err = ParseConstraints(legacyZone.ReplicaAttrs[0].Attrs)
			if err != nil {
				return ZoneConfig{}, err
			}
		}
	}
	return zone, nil
}
