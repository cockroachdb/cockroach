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

import (
	"errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// MigrateZoneConfig migrates the legacy ZoneConfig format into the new one.
func MigrateZoneConfig(value *roachpb.Value) (ZoneConfig, error) {
	var zone ZoneConfig
	if err := value.GetProto(&zone); err != nil {
		return ZoneConfig{}, err
	}
	if len(zone.ReplicaAttrs) > 0 {
		if zone.NumReplicas > 0 || len(zone.Constraints.Constraints) > 0 {
			return ZoneConfig{}, errors.New("migration to new ZoneConfig failed due to previous partial upgrade")
		}
		zone.NumReplicas = int32(len(zone.ReplicaAttrs))
		if zone.NumReplicas > 0 {
			attrs := zone.ReplicaAttrs[0].Attrs
			zone.Constraints.Constraints = make([]Constraint, len(attrs))
			for i, attr := range attrs {
				zone.Constraints.Constraints[i].Value = attr
			}
		}
		zone.ReplicaAttrs = nil
	}
	return zone, nil
}
