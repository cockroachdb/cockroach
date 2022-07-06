// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zoneconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// ZoneConfigReader implements the scbuild.ZoneConfigReader interface
type ZoneConfigReader struct {
	txn   *kv.Txn
	codec keys.SQLCodec
}

// NewZoneConfigReader constructs a new zone config reader for execution.
func NewZoneConfigReader(txn *kv.Txn, codec keys.SQLCodec) *ZoneConfigReader {
	return &ZoneConfigReader{
		txn:   txn,
		codec: codec,
	}
}

// GetZoneConfig reads the zone config the system table.
func (zc *ZoneConfigReader) GetZoneConfig(
	ctx context.Context, id descpb.ID,
) (*zonepb.ZoneConfig, error) {
	kv, err := zc.txn.Get(ctx, config.MakeZoneKey(zc.codec, id))
	if err != nil {
		return nil, err
	}
	if kv.Value == nil {
		return nil, nil
	}
	var zone zonepb.ZoneConfig
	if err := kv.ValueProto(&zone); err != nil {
		return nil, err
	}
	return &zone, nil
}
