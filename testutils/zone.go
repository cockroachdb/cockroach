// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package testutils

import (
	"reflect"

	gogoproto "github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
)

// DB provides the functionality needed to read and write zone configs.
type DB interface {
	GetProto(interface{}, gogoproto.Message) error
	Put(interface{}, interface{}) error
}

func modifyDefaultZoneConfig(db DB, fn func(proto.ZoneConfig) proto.ZoneConfig) error {
	zone := proto.ZoneConfig{}
	if err := db.GetProto(keys.ConfigZonePrefix, &zone); err != nil {
		return err
	}
	newZone := fn(zone)
	if reflect.DeepEqual(zone, newZone) {
		return nil
	}
	return db.Put(keys.ConfigZonePrefix, &newZone)
}

// SetDefaultRangeMaxBytes sets the range-max-bytes value for the default zone.
func SetDefaultRangeMaxBytes(db DB, maxBytes int64) error {
	return modifyDefaultZoneConfig(db, func(zone proto.ZoneConfig) proto.ZoneConfig {
		zone.RangeMaxBytes = maxBytes
		return zone
	})
}

// SetDefaultRangeReplicaNum sets the replication factor for the default zone.
func SetDefaultRangeReplicaNum(db DB, numReplicas int) error {
	return modifyDefaultZoneConfig(db, func(zone proto.ZoneConfig) proto.ZoneConfig {
		if len(zone.ReplicaAttrs) > numReplicas {
			zone.ReplicaAttrs = zone.ReplicaAttrs[:numReplicas]
		}
		return zone
	})
}
