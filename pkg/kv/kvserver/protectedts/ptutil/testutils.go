// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TestingVerifyProtectionTimestampExistsOnSpans refreshes the PTS state in KV and
// ensures a protection at the given protectionTimestamp exists for all the
// supplied spans.
func TestingVerifyProtectionTimestampExistsOnSpans(
	ctx context.Context,
	t *testing.T,
	srv serverutils.TestServerInterface,
	ptsReader spanconfig.ProtectedTSReader,
	protectionTimestamp hlc.Timestamp,
	spans roachpb.Spans,
) error {
	testutils.SucceedsSoon(t, func() error {
		if err := spanconfigptsreader.TestingRefreshPTSState(
			ctx, t, ptsReader, srv.Clock().Now(),
		); err != nil {
			return err
		}
		for _, sp := range spans {
			timestamps, _, err := ptsReader.GetProtectionTimestamps(ctx, sp)
			if err != nil {
				return err
			}
			found := false
			for _, ts := range timestamps {
				if ts.Equal(protectionTimestamp) {
					found = true
					break
				}
			}
			if !found {
				return errors.Newf("protection timestamp %s does not exist on span %s", protectionTimestamp, sp)
			}
		}
		return nil
	})
	return nil
}

func GetPTSTarget(t *testing.T, db *sqlutils.SQLRunner, ptsID *uuid.UUID) *ptpb.Target {
	ret := &ptpb.Target{}
	var buf []byte
	db.QueryRow(t, `SELECT target FROM system.protected_ts_records WHERE id = $1`, ptsID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, ret); err != nil {
		t.Fatal(err)
	}
	return ret
}
