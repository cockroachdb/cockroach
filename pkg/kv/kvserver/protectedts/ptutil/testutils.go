// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// TestingWaitForProtectedTimestampToExistOnSpans waits
// testutils.SucceedsSoonDuration for the supplied protected timestamp to exist
// on all the supplied spans. It fatals if this doesn't happen in time.
func TestingWaitForProtectedTimestampToExistOnSpans(
	ctx context.Context,
	t *testing.T,
	srv serverutils.TestServerInterface,
	ptsReader spanconfig.ProtectedTSReader,
	protectedTimestamp hlc.Timestamp,
	spans roachpb.Spans,
) {
	testutils.SucceedsSoon(t, func() error {
		if err := spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, srv.Clock().Now()); err != nil {
			return err
		}
		for _, sp := range spans {
			timestamps, _, err := ptsReader.GetProtectionTimestamps(ctx, sp)
			if err != nil {
				return err
			}
			found := false
			for _, ts := range timestamps {
				if ts.Equal(protectedTimestamp) {
					found = true
					break
				}
			}
			if !found {
				return errors.Newf("protection timestamp %s does not exist on span %s", protectedTimestamp, sp)
			}
		}
		return nil
	})
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
