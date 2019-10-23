// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ForceTableGC sends a GCRequest for the ranges corresponding to a table.
func ForceTableGC(
	t testing.TB,
	distSender *kv.DistSender,
	db DBHandle,
	database, table string,
	timestamp hlc.Timestamp,
) {
	t.Helper()
	tblID := QueryTableID(t, db, database, table)
	tblKey := roachpb.Key(keys.MakeTablePrefix(tblID))
	gcr := roachpb.GCRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    tblKey,
			EndKey: tblKey.PrefixEnd(),
		},
		Threshold: timestamp,
	}
	if _, err := client.SendWrapped(
		context.Background(), distSender, &gcr,
	); err != nil {
		t.Error(err)
	}
}
