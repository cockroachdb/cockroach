// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descmetadata

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// ZoneConfigGetter implements the scbuild.ZoneConfigGetter interface
type ZoneConfigGetter struct {
	ie  sqlutil.InternalExecutor
	txn *kv.Txn
}

// NewZoneConfigGetter constructs a new zone config reader for execution.
func NewZoneConfigGetter(txn *kv.Txn, ie sqlutil.InternalExecutor) *ZoneConfigGetter {
	return &ZoneConfigGetter{
		ie:  ie,
		txn: txn,
	}
}

// GetZoneConfig reads the zone config the system table.
func (zc *ZoneConfigGetter) GetZoneConfig(
	ctx context.Context, id descpb.ID,
) (*zonepb.ZoneConfig, error) {
	datums, err := zc.ie.QueryRow(ctx, "read-zone-config", zc.txn,
		"SELECT config FROM system.zones WHERE	 id=$1", id)
	if err != nil {
		return nil, err
	}
	if len(datums) == 0 || datums[0] == tree.DNull {
		return nil, nil
	}
	bytes, ok := datums[0].(*tree.DBytes)
	if !ok {
		return nil, errors.AssertionFailedf("failed to retrieve zone config, unexpected datum %v",
			datums)
	}
	var zone zonepb.ZoneConfig
	err = protoutil.Unmarshal([]byte(*bytes), &zone)
	if err != nil {
		return nil, err
	}
	return &zone, nil
}
