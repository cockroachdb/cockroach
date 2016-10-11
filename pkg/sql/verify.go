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
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func expectDescriptorID(systemConfig config.SystemConfig, idKey roachpb.Key, id sqlbase.ID) error {
	idValue := systemConfig.GetValue(idKey)
	if idValue == nil {
		return errStaleMetadata
	}
	cachedID, err := idValue.GetInt()
	if err != nil {
		return err
	}
	if sqlbase.ID(cachedID) != id {
		return errStaleMetadata
	}
	return nil
}

func expectDescriptor(
	systemConfig config.SystemConfig, idKey roachpb.Key, desc *sqlbase.Descriptor,
) error {
	descValue := systemConfig.GetValue(idKey)
	if descValue == nil {
		return errStaleMetadata
	}
	var cachedDesc sqlbase.Descriptor
	if err := descValue.GetProto(&cachedDesc); err != nil {
		return err
	}
	if !proto.Equal(&cachedDesc, desc) {
		return errStaleMetadata
	}
	return nil
}

func expectDeleted(systemConfig config.SystemConfig, key roachpb.Key) error {
	if systemConfig.GetValue(key) != nil {
		return errStaleMetadata
	}
	return nil
}
