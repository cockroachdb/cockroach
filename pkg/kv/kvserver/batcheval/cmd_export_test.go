// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestExportCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	export := func(
		t *testing.T, startTime, endTime hlc.Timestamp, startKey roachpb.Key, mvccFilter roachpb.MVCCFilter,
	) (roachpb.Response, *roachpb.Error) {
		req := &roachpb.ExportRequest{
			RequestHeader:             roachpb.RequestHeader{Key: startKey, EndKey: keys.MaxKey},
			StartTime:                 startTime,
			MVCCFilter:                mvccFilter,
			SplitMidKey:               true,
			DeprecatedTargetFileSize:  batcheval.ExportRequestTargetFileSize.Get(&tc.Server(0).ClusterSettings().SV),
			MaxAllowedFileSizeOverage: batcheval.ExportRequestMaxAllowedFileSizeOverage.Get(&tc.Server(0).ClusterSettings().SV),
		}
		var h roachpb.Header
		h.TargetBytes = batcheval.ExportRequestTargetFileSize.Get(&tc.Server(0).ClusterSettings().SV)
		h.Timestamp = endTime
		return kv.SendWrappedWith(ctx, kvDB.NonTransactionalSender(), h, req)
	}

	// exportAndSlurpOne sends a single ExportRequest and processes its response.
	exportAndSlurpOne := func(
		t *testing.T, startTime, endTime hlc.Timestamp, startKey roachpb.Key, mvccFilter roachpb.MVCCFilter,
	) ([]string, []storage.MVCCKeyValue, roachpb.ResponseHeader) {
		res, pErr := export(t, startTime, endTime, startKey, mvccFilter)
		if pErr != nil {
			t.Fatalf("%+v", pErr)
		}
		var paths []string
		var kvs []storage.MVCCKeyValue
		for _, file := range res.(*roachpb.ExportResponse).Files {
			paths = append(paths, file.Path)
			iterOpts := storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsOnly,
				LowerBound: keys.LocalMax,
				UpperBound: keys.MaxKey,
			}
			sst, err := storage.NewMemSSTIterator(file.SST, true, iterOpts)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			defer sst.Close()
			sst.SeekGE(storage.MVCCKey{Key: keys.MinKey})
			for {
				if valid, err := sst.Valid(); !valid || err != nil {
					if err != nil {
						t.Fatalf("%+v", err)
					}
					break
				}
				newKv := storage.MVCCKeyValue{}
				newKv.Key.Key = append(newKv.Key.Key, sst.UnsafeKey().Key...)
				newKv.Key.Timestamp = sst.UnsafeKey().Timestamp
				v, err := sst.UnsafeValue()
				require.NoError(t, err)
				newKv.Value = append(newKv.Value, v...)
				kvs = append(kvs, newKv)
				sst.Next()
			}
		}
		return paths, kvs, res.(*roachpb.ExportResponse).Header()
	}
	type exportAndSlurpResult struct {
		end                      hlc.Timestamp
		filter                   roachpb.MVCCFilter
		mvccLatestFiles          []string
		mvccLatestKVs            []storage.MVCCKeyValue
		mvccAllFiles             []string
		mvccAllKVs               []storage.MVCCKeyValue
		mvccLatestResponseHeader roachpb.ResponseHeader
		mvccAllResponseHeader    roachpb.ResponseHeader
		resumeSpans              []string
	}
	// exportAndSlurp runs ExportRequests until all resume spans have been
	// exhausted. It returns an ExportAndSlurpResult aggregated across
	// ExportRequests.
	exportAndSlurp := func(t *testing.T, start hlc.Timestamp, filter roachpb.MVCCFilter) exportAndSlurpResult {
		var ret exportAndSlurpResult
		ret.end = hlc.NewClockWithSystemTimeSource(time.Nanosecond).Now( /* maxOffset */ )
		ret.filter = filter
		todo := make(chan roachpb.Key, 1)
		todo <- bootstrap.TestingUserTableDataMin()
		for {
			select {
			case span := <-todo:
				mvccFiles, mvccKVs, respHeader := exportAndSlurpOne(t, start, ret.end, span, filter)
				switch filter {
				case roachpb.MVCCFilter_All:
					ret.mvccAllFiles = append(ret.mvccAllFiles, mvccFiles...)
					ret.mvccAllKVs = append(ret.mvccAllKVs, mvccKVs...)
				case roachpb.MVCCFilter_Latest:
					ret.mvccLatestFiles = append(ret.mvccLatestFiles, mvccFiles...)
					ret.mvccLatestKVs = append(ret.mvccLatestKVs, mvccKVs...)
				}
				if respHeader.ResumeSpan != nil {
					ret.resumeSpans = append(ret.resumeSpans, respHeader.ResumeSpan.String())
					todo <- respHeader.ResumeSpan.Key
				}
			default:
				return ret
			}
		}
	}

	expect := func(
		t *testing.T, res exportAndSlurpResult,
		mvccFilesLen int, mvccKVsLen int, resumeSpans ...string,
	) {
		t.Helper()
		switch res.filter {
		case roachpb.MVCCFilter_Latest:
			require.Len(t, res.mvccLatestFiles, mvccFilesLen, "unexpected files in latest export")
			require.Len(t, res.mvccLatestKVs, mvccKVsLen, "unexpected kvs in latest export")
		case roachpb.MVCCFilter_All:
			require.Len(t, res.mvccAllFiles, mvccFilesLen, "unexpected files in all export")
			require.Len(t, res.mvccAllKVs, mvccKVsLen, "unexpected kvs in all export")
		default:
			t.Fatal("unknown filter value")
		}
		require.Equal(t, resumeSpans, res.resumeSpans)
	}

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE mvcclatest`)
	sqlDB.Exec(t, `CREATE TABLE mvcclatest.export (id INT PRIMARY KEY, value INT)`)

	const (
		targetSizeSetting = "kv.bulk_sst.target_size"
		maxOverageSetting = "kv.bulk_sst.max_allowed_overage"
	)
	var (
		setSetting = func(t *testing.T, variable, val string) {
			sqlDB.Exec(t, "SET CLUSTER SETTING "+variable+" = "+val)
		}
		resetSetting = func(t *testing.T, variable string) {
			setSetting(t, variable, "DEFAULT")
		}
		setExportTargetSize = func(t *testing.T, val string) {
			setSetting(t, targetSizeSetting, val)
		}
		resetExportTargetSize = func(t *testing.T) {
			resetSetting(t, targetSizeSetting)
		}
		setMaxOverage = func(t *testing.T, val string) {
			setSetting(t, maxOverageSetting, val)
		}
		resetMaxOverage = func(t *testing.T) {
			resetSetting(t, maxOverageSetting)
		}
	)

	var endTimeTS1 hlc.Timestamp
	t.Run("ts1", func(t *testing.T) {
		// When run with MVCCFilter_Latest and a startTime of 0 (full backup of only
		// the latest values), Export special cases and skips keys that are deleted
		// before the export timestamp.
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (1, 1), (3, 3), (4, 4)`)
		sqlDB.Exec(t, `DELETE from mvcclatest.export WHERE id = 4`)
		latest := exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_Latest)
		expect(t, latest, 1, 2)
		all := exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_All)
		expect(t, all, 1, 4)

		// At this point `mvcclatest.export` has:
		//
		// (1, 1):latest
		// (3, 3):latest
		// deletionTombstone:latest <- (4, 4)
		//
		// The total size of the latest KV revisions in `mvcclatest.export` is 22
		// bytes (2 KVs * 11 bytes), and the total size of all KV revisions is 37
		// bytes (22 bytes + 4 bytes for the deletion tombstone + 11 bytes for the
		// older revision).
		//
		// Setting the target file size to 1 byte means that we will only fit one KV
		// of size 11 bytes in the SST returned by a single ExportRequest because
		// that is all we can fit in `kv.bulk_sst.target_size` +
		// `kv.bulk_sst.max_allowed_overage`.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		latest = exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_Latest)
		expect(t, latest, 2, 2, []string{"/{Table/106/1/3/0-Max}"}...)
		all = exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_All)
		expect(t, all, 3, 4, []string{"/{Table/106/1/3/0-Max}", "/{Table/106/1/4/0-Max}"}...)
		endTimeTS1 = latest.end
	})

	var endTimeTS2 hlc.Timestamp
	t.Run("ts2", func(t *testing.T) {
		// If nothing has changed since the last ExportRequest, nothing should be
		// exported.
		latest := exportAndSlurp(t, endTimeTS1, roachpb.MVCCFilter_Latest)
		expect(t, latest, 0, 0)
		all := exportAndSlurp(t, endTimeTS1, roachpb.MVCCFilter_All)
		expect(t, all, 0, 0)
		endTimeTS2 = latest.end
	})

	var endTimeTS3 hlc.Timestamp
	t.Run("ts3", func(t *testing.T) {
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (2, 2)`)
		sqlDB.Exec(t, `UPSERT INTO mvcclatest.export VALUES (2, 8)`)

		// At this point `mvcclatest.export` has:
		//
		// (1, 1):latest
		// (2, 8):latest <- (2, 2)
		// (3, 3):latest
		// deletionTombstone:latest <- (4, 4)
		//
		// With target size and max overage set to their defaults of 16MiB and 64MiB
		// we should fit all KVs in a single file.
		latest := exportAndSlurp(t, endTimeTS2, roachpb.MVCCFilter_Latest)
		expect(t, latest, 1, 1)
		all := exportAndSlurp(t, endTimeTS2, roachpb.MVCCFilter_All)
		expect(t, all, 1, 2)
		endTimeTS3 = latest.end
	})

	t.Run("ts4", func(t *testing.T) {
		sqlDB.Exec(t, `DELETE FROM mvcclatest.export WHERE id = 3`)

		// At this point `mvcclatest.export` has:
		//
		// (1, 1):latest
		// (2, 8):latest <- (2, 2)
		// deletionTombstone:latest <- (3, 3)
		// deletionTombstone:latest <- (4, 4)
		//
		// With `endTimeTS3` as the StartTime both ExportRequests will see only the
		// deletion tombstone that fits in our default target size of 16MiB.
		latest := exportAndSlurp(t, endTimeTS3, roachpb.MVCCFilter_Latest)
		expect(t, latest, 1, 1)
		all := exportAndSlurp(t, endTimeTS3, roachpb.MVCCFilter_All)
		expect(t, all, 1, 1)
		if len(latest.mvccLatestKVs[0].Value) != 0 {
			v := roachpb.Value{RawBytes: latest.mvccLatestKVs[0].Value}
			t.Errorf("expected a deletion tombstone got %s", v.PrettyPrint())
		}
		if len(all.mvccAllKVs[0].Value) != 0 {
			v := roachpb.Value{RawBytes: all.mvccAllKVs[0].Value}
			t.Errorf("expected a deletion tombstone got %s", v.PrettyPrint())
		}
	})

	var endTimeTS5 hlc.Timestamp
	t.Run("ts5", func(t *testing.T) {
		sqlDB.Exec(t, `ALTER TABLE mvcclatest.export SPLIT AT VALUES (2)`)

		// At this point `mvcclatest.export` has:
		// (1, 1):latest
		// (2, 8):latest <- (2,2)
		// deletionTombstone <- (3, 3)
		// deletionTombstone <- (4, 4)
		latest := exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_Latest)
		expect(t, latest, 2, 2)
		all := exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_All)
		expect(t, all, 2, 7)

		// Re-run the test with a 1b target size which will result in a single KV
		// (and its revisions) per file.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		latest = exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_Latest)
		expect(t, latest, 2, 2, []string{"/{Table/106/1/2-Max}"}...)
		all = exportAndSlurp(t, hlc.Timestamp{}, roachpb.MVCCFilter_All)
		expect(t, all, 4, 7, []string{"/{Table/106/1/2-Max}", "/{Table/106/1/3/0-Max}", "/{Table/106/1/4/0-Max}"}...)
		endTimeTS5 = all.end
	})

	t.Run("ts6", func(t *testing.T) {
		// Add 100 rows to the table.
		sqlDB.Exec(t, `WITH RECURSIVE
	   t (id, value)
	       AS (VALUES (1, 1) UNION ALL SELECT id + 1, value FROM t WHERE id < 100)
	UPSERT
	INTO
	   mvcclatest.export
	(SELECT id, value FROM t);`)

		// Run the test with the default target size of 16 MiB which will lead to 2
		// files due to the above range split. Note how we do not see any resume
		// spans because the dist sender divides the ExportRequest to send to the
		// LHS and RHS of the split range and then combines the responses from both
		// ranges before returning.
		latest := exportAndSlurp(t, endTimeTS5, roachpb.MVCCFilter_Latest)
		expect(t, latest, 2, 100)
		all := exportAndSlurp(t, endTimeTS5, roachpb.MVCCFilter_All)
		expect(t, all, 2, 100)

		// Re-run the test with a 1b target size which means that only a single KV
		// (and its revisions) will fit in an SST before we paginate.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		latest = exportAndSlurp(t, endTimeTS5, roachpb.MVCCFilter_Latest)
		expect(t, latest, 100, 100, []string{"/{Table/106/1/2-Max}", "/{Table/106/1/3/0-Max}", "/{Table/106/1/4/0-Max}", "/{Table/106/1/5/0-Max}", "/{Table/106/1/6/0-Max}", "/{Table/106/1/7/0-Max}", "/{Table/106/1/8/0-Max}",
			"/{Table/106/1/9/0-Max}", "/{Table/106/1/10/0-Max}", "/{Table/106/1/11/0-Max}", "/{Table/106/1/12/0-Max}", "/{Table/106/1/13/0-Max}", "/{Table/106/1/14/0-Max}", "/{Table/106/1/15/0-Max}", "/{Table/106/1/16/0-Max}", "/{Table/106/1/17/0-Max}",
			"/{Table/106/1/18/0-Max}", "/{Table/106/1/19/0-Max}", "/{Table/106/1/20/0-Max}", "/{Table/106/1/21/0-Max}", "/{Table/106/1/22/0-Max}", "/{Table/106/1/23/0-Max}", "/{Table/106/1/24/0-Max}", "/{Table/106/1/25/0-Max}", "/{Table/106/1/26/0-Max}",
			"/{Table/106/1/27/0-Max}", "/{Table/106/1/28/0-Max}", "/{Table/106/1/29/0-Max}", "/{Table/106/1/30/0-Max}", "/{Table/106/1/31/0-Max}", "/{Table/106/1/32/0-Max}", "/{Table/106/1/33/0-Max}", "/{Table/106/1/34/0-Max}", "/{Table/106/1/35/0-Max}",
			"/{Table/106/1/36/0-Max}", "/{Table/106/1/37/0-Max}", "/{Table/106/1/38/0-Max}", "/{Table/106/1/39/0-Max}", "/{Table/106/1/40/0-Max}", "/{Table/106/1/41/0-Max}", "/{Table/106/1/42/0-Max}", "/{Table/106/1/43/0-Max}",
			"/{Table/106/1/44/0-Max}", "/{Table/106/1/45/0-Max}", "/{Table/106/1/46/0-Max}", "/{Table/106/1/47/0-Max}", "/{Table/106/1/48/0-Max}", "/{Table/106/1/49/0-Max}", "/{Table/106/1/50/0-Max}", "/{Table/106/1/51/0-Max}", "/{Table/106/1/52/0-Max}",
			"/{Table/106/1/53/0-Max}", "/{Table/106/1/54/0-Max}", "/{Table/106/1/55/0-Max}", "/{Table/106/1/56/0-Max}", "/{Table/106/1/57/0-Max}", "/{Table/106/1/58/0-Max}", "/{Table/106/1/59/0-Max}", "/{Table/106/1/60/0-Max}", "/{Table/106/1/61/0-Max}",
			"/{Table/106/1/62/0-Max}", "/{Table/106/1/63/0-Max}", "/{Table/106/1/64/0-Max}", "/{Table/106/1/65/0-Max}", "/{Table/106/1/66/0-Max}", "/{Table/106/1/67/0-Max}", "/{Table/106/1/68/0-Max}", "/{Table/106/1/69/0-Max}", "/{Table/106/1/70/0-Max}",
			"/{Table/106/1/71/0-Max}", "/{Table/106/1/72/0-Max}", "/{Table/106/1/73/0-Max}", "/{Table/106/1/74/0-Max}", "/{Table/106/1/75/0-Max}", "/{Table/106/1/76/0-Max}", "/{Table/106/1/77/0-Max}", "/{Table/106/1/78/0-Max}", "/{Table/106/1/79/0-Max}",
			"/{Table/106/1/80/0-Max}", "/{Table/106/1/81/0-Max}", "/{Table/106/1/82/0-Max}", "/{Table/106/1/83/0-Max}", "/{Table/106/1/84/0-Max}", "/{Table/106/1/85/0-Max}", "/{Table/106/1/86/0-Max}", "/{Table/106/1/87/0-Max}", "/{Table/106/1/88/0-Max}",
			"/{Table/106/1/89/0-Max}", "/{Table/106/1/90/0-Max}", "/{Table/106/1/91/0-Max}", "/{Table/106/1/92/0-Max}", "/{Table/106/1/93/0-Max}", "/{Table/106/1/94/0-Max}", "/{Table/106/1/95/0-Max}", "/{Table/106/1/96/0-Max}", "/{Table/106/1/97/0-Max}",
			"/{Table/106/1/98/0-Max}", "/{Table/106/1/99/0-Max}", "/{Table/106/1/100/0-Max}"}...)
		all = exportAndSlurp(t, endTimeTS5, roachpb.MVCCFilter_All)
		expect(t, all, 100, 100, []string{"/{Table/106/1/2-Max}", "/{Table/106/1/3/0-Max}", "/{Table/106/1/4/0-Max}", "/{Table/106/1/5/0-Max}", "/{Table/106/1/6/0-Max}", "/{Table/106/1/7/0-Max}", "/{Table/106/1/8/0-Max}",
			"/{Table/106/1/9/0-Max}", "/{Table/106/1/10/0-Max}", "/{Table/106/1/11/0-Max}", "/{Table/106/1/12/0-Max}", "/{Table/106/1/13/0-Max}", "/{Table/106/1/14/0-Max}", "/{Table/106/1/15/0-Max}", "/{Table/106/1/16/0-Max}", "/{Table/106/1/17/0-Max}",
			"/{Table/106/1/18/0-Max}", "/{Table/106/1/19/0-Max}", "/{Table/106/1/20/0-Max}", "/{Table/106/1/21/0-Max}", "/{Table/106/1/22/0-Max}", "/{Table/106/1/23/0-Max}", "/{Table/106/1/24/0-Max}", "/{Table/106/1/25/0-Max}", "/{Table/106/1/26/0-Max}",
			"/{Table/106/1/27/0-Max}", "/{Table/106/1/28/0-Max}", "/{Table/106/1/29/0-Max}", "/{Table/106/1/30/0-Max}", "/{Table/106/1/31/0-Max}", "/{Table/106/1/32/0-Max}", "/{Table/106/1/33/0-Max}", "/{Table/106/1/34/0-Max}", "/{Table/106/1/35/0-Max}",
			"/{Table/106/1/36/0-Max}", "/{Table/106/1/37/0-Max}", "/{Table/106/1/38/0-Max}", "/{Table/106/1/39/0-Max}", "/{Table/106/1/40/0-Max}", "/{Table/106/1/41/0-Max}", "/{Table/106/1/42/0-Max}", "/{Table/106/1/43/0-Max}",
			"/{Table/106/1/44/0-Max}", "/{Table/106/1/45/0-Max}", "/{Table/106/1/46/0-Max}", "/{Table/106/1/47/0-Max}", "/{Table/106/1/48/0-Max}", "/{Table/106/1/49/0-Max}", "/{Table/106/1/50/0-Max}", "/{Table/106/1/51/0-Max}", "/{Table/106/1/52/0-Max}",
			"/{Table/106/1/53/0-Max}", "/{Table/106/1/54/0-Max}", "/{Table/106/1/55/0-Max}", "/{Table/106/1/56/0-Max}", "/{Table/106/1/57/0-Max}", "/{Table/106/1/58/0-Max}", "/{Table/106/1/59/0-Max}", "/{Table/106/1/60/0-Max}", "/{Table/106/1/61/0-Max}",
			"/{Table/106/1/62/0-Max}", "/{Table/106/1/63/0-Max}", "/{Table/106/1/64/0-Max}", "/{Table/106/1/65/0-Max}", "/{Table/106/1/66/0-Max}", "/{Table/106/1/67/0-Max}", "/{Table/106/1/68/0-Max}", "/{Table/106/1/69/0-Max}", "/{Table/106/1/70/0-Max}",
			"/{Table/106/1/71/0-Max}", "/{Table/106/1/72/0-Max}", "/{Table/106/1/73/0-Max}", "/{Table/106/1/74/0-Max}", "/{Table/106/1/75/0-Max}", "/{Table/106/1/76/0-Max}", "/{Table/106/1/77/0-Max}", "/{Table/106/1/78/0-Max}", "/{Table/106/1/79/0-Max}",
			"/{Table/106/1/80/0-Max}", "/{Table/106/1/81/0-Max}", "/{Table/106/1/82/0-Max}", "/{Table/106/1/83/0-Max}", "/{Table/106/1/84/0-Max}", "/{Table/106/1/85/0-Max}", "/{Table/106/1/86/0-Max}", "/{Table/106/1/87/0-Max}", "/{Table/106/1/88/0-Max}",
			"/{Table/106/1/89/0-Max}", "/{Table/106/1/90/0-Max}", "/{Table/106/1/91/0-Max}", "/{Table/106/1/92/0-Max}", "/{Table/106/1/93/0-Max}", "/{Table/106/1/94/0-Max}", "/{Table/106/1/95/0-Max}", "/{Table/106/1/96/0-Max}", "/{Table/106/1/97/0-Max}",
			"/{Table/106/1/98/0-Max}", "/{Table/106/1/99/0-Max}", "/{Table/106/1/100/0-Max}"}...)

		// Set `kv.bulk_sst.max_allowed_overage` to 1b and ensure that we get errors
		// due to the max size of a KV (and its revisions) being exceeded.
		defer resetMaxOverage(t)
		setMaxOverage(t, "'1b'")
		_, pErr := export(t, endTimeTS5,
			hlc.NewClockWithSystemTimeSource(time.Nanosecond).Now( /* maxOffset */ ),
			bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_Latest)
		const expectedError = `export size \(11 bytes\) exceeds max size \(2 bytes\)`
		require.Regexp(t, expectedError, pErr)
		hints := errors.GetAllHints(pErr.GoError())
		require.Equal(t, 1, len(hints))
		const expectedHint = `consider increasing cluster setting "kv.bulk_sst.max_allowed_overage"`
		require.Regexp(t, expectedHint, hints[0])
		_, pErr = export(t, endTimeTS5, hlc.NewClockWithSystemTimeSource(time.Nanosecond).Now( /* maxOffset */ ),
			bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_All)
		require.Regexp(t, expectedError, pErr)

		// Disable `kv.bulk_sst.target_size` and ensure that we don't get any errors
		// to the max overage being exceeded.
		setExportTargetSize(t, "'0b'")
		latest = exportAndSlurp(t, endTimeTS5, roachpb.MVCCFilter_Latest)
		expect(t, latest, 2, 100)
		all = exportAndSlurp(t, endTimeTS5, roachpb.MVCCFilter_All)
		expect(t, all, 2, 100)
	})

	t.Run("ts7", func(t *testing.T) {
		// Because of the above split, there are going to be two ExportRequests by
		// the DistSender. One for the first KV and the next one for the remaining
		// KVs. This allows us to test both the TargetBytes limit within a single
		// export request and across subsequent requests.

		// Setting `kv.bulk_sst.target_size` to the size of a single KV.
		//
		// The ExportRequest to the first range will return a single KV SST (11
		// bytes), and reduce the byte limit for the ExportRequest to the next range
		// to 0.
		endTime := hlc.NewClockWithSystemTimeSource(time.Nanosecond).Now( /* maxOffset */ )
		setExportTargetSize(t, "'11b'")
		resp, pErr := export(t, endTimeTS5, endTime, bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_Latest)
		require.NoError(t, pErr.GoError())
		require.Equal(t, int64(11), resp.(*roachpb.ExportResponse).NumBytes)
		require.Equal(t, "/{Table/106/1/2-Max}", resp.(*roachpb.ExportResponse).ResumeSpan.String())

		// Setting `kv.bulk_sst.target_size` to a size greater than a single KV.
		//
		// The ExportRequest to the first range will return an SST with one KV (11
		// bytes), this reduces the TargetBytes for the ExportRequest to the next
		// range to 1 byte which means we will fit 1 KV in the SST before
		// paginating.
		setExportTargetSize(t, "'12b'")
		resp, pErr = export(t, endTimeTS5, endTime, bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_Latest)
		require.NoError(t, pErr.GoError())
		require.Equal(t, int64(22), resp.(*roachpb.ExportResponse).NumBytes)
		require.Equal(t, "/{Table/106/1/3/0-Max}", resp.(*roachpb.ExportResponse).ResumeSpan.String())

		// Setting `kv.bulk_sst.target_size` to a size greater than all KVs.
		setExportTargetSize(t, "'1111b'")
		resp, pErr = export(t, endTimeTS5, endTime, bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_Latest)
		require.NoError(t, pErr.GoError())
		require.Equal(t, int64(1100), resp.(*roachpb.ExportResponse).NumBytes)
		require.Nil(t, resp.(*roachpb.ExportResponse).ResumeSpan)

		// Setting `kv.bulk_sst.target_size` to a size just less than all KVs.
		setExportTargetSize(t, "'1089b'")
		resp, pErr = export(t, endTimeTS5, endTime, bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_Latest)
		require.NoError(t, pErr.GoError())
		require.Equal(t, int64(1089), resp.(*roachpb.ExportResponse).NumBytes)
		require.Equal(t, resp.(*roachpb.ExportResponse).ResumeSpan.String(), "/{Table/106/1/100/0-Max}")

		// Setting `kv.bulk_sst.target_size` to the size of two KVs.
		//
		// The ExportRequest to the first range will return an SST with one KV (11
		// bytes), this reduces the TargetBytes for the ExportRequest to the next
		// range to 11 bytes which means we will fit 1 KV in the SST before
		// paginating.
		setExportTargetSize(t, "'22b'")
		resp, pErr = export(t, endTimeTS5, endTime, bootstrap.TestingUserTableDataMin(), roachpb.MVCCFilter_Latest)
		require.NoError(t, pErr.GoError())
		require.Equal(t, int64(22), resp.(*roachpb.ExportResponse).NumBytes)
		require.Equal(t, "/{Table/106/1/3/0-Max}", resp.(*roachpb.ExportResponse).ResumeSpan.String())
	})
}

func TestExportGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeader{Key: bootstrap.TestingUserTableDataMin(), EndKey: keys.MaxKey},
		StartTime:     hlc.Timestamp{WallTime: -1},
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
}

// exportUsingGoIterator uses the legacy implementation of export, and is used
// as an oracle to check the correctness of pebbleExportToSst.
func exportUsingGoIterator(
	ctx context.Context,
	filter roachpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	startKey, endKey roachpb.Key,
	reader storage.Reader,
) ([]byte, error) {
	memFile := &storage.MemFile{}
	sst := storage.MakeIngestionSSTWriter(
		ctx, cluster.MakeTestingClusterSettings(), memFile,
	)
	defer sst.Close()

	var skipTombstones bool
	var iterFn func(*storage.MVCCIncrementalIterator)
	switch filter {
	case roachpb.MVCCFilter_Latest:
		skipTombstones = true
		iterFn = (*storage.MVCCIncrementalIterator).NextKey
	case roachpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*storage.MVCCIncrementalIterator).Next
	default:
		return nil, nil
	}

	iter := storage.NewMVCCIncrementalIterator(reader, storage.MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	defer iter.Close()
	for iter.SeekGE(storage.MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return nil, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}

		// Skip tombstone (len=0) records when startTime is zero
		// (non-incremental) and we're not exporting all versions.
		v, err := iter.UnsafeValue()
		if err != nil {
			return nil, err
		}
		if skipTombstones && startTime.IsEmpty() && len(v) == 0 {
			continue
		}

		if err := sst.Put(iter.UnsafeKey(), v); err != nil {
			return nil, err
		}
	}
	if err := sst.Finish(); err != nil {
		return nil, err
	}

	if len(memFile.Data()) == 0 {
		// Let the defer Close the sstable.
		return nil, nil
	}

	return memFile.Data(), nil
}

func loadSST(t *testing.T, data []byte, start, end roachpb.Key) []storage.MVCCKeyValue {
	t.Helper()
	if len(data) == 0 {
		return nil
	}

	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: start,
		UpperBound: end,
	}
	sst, err := storage.NewMemSSTIterator(data, true, iterOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	var kvs []storage.MVCCKeyValue
	sst.SeekGE(storage.MVCCKey{Key: start})
	for {
		if valid, err := sst.Valid(); !valid || err != nil {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !sst.UnsafeKey().Less(storage.MVCCKey{Key: end}) {
			break
		}
		newKv := storage.MVCCKeyValue{}
		newKv.Key.Key = append(newKv.Key.Key, sst.UnsafeKey().Key...)
		newKv.Key.Timestamp = sst.UnsafeKey().Timestamp
		v, err := sst.UnsafeValue()
		if err != nil {
			t.Fatal(err)
		}
		newKv.Value = append(newKv.Value, v...)
		kvs = append(kvs, newKv)
		sst.Next()
	}

	return kvs
}

type exportRevisions bool
type batchBoundaries bool

const (
	exportAll    exportRevisions = true
	exportLatest exportRevisions = false

	stopAtTimestamps batchBoundaries = true
	stopAtKeys       batchBoundaries = false
)

func assertEqualKVs(
	ctx context.Context,
	st *cluster.Settings,
	e storage.Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	exportAllRevisions exportRevisions,
	stopMidKey batchBoundaries,
	targetSize uint64,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		var filter roachpb.MVCCFilter
		if exportAllRevisions {
			filter = roachpb.MVCCFilter_All
		} else {
			filter = roachpb.MVCCFilter_Latest
		}

		// Run the oracle which is a legacy implementation of pebbleExportToSst
		// backed by an MVCCIncrementalIterator.
		expected, err := exportUsingGoIterator(ctx, filter, startTime, endTime, startKey, endKey, e)
		if err != nil {
			t.Fatalf("Oracle failed to export provided key range.")
		}

		// Run the actual code path used when exporting MVCCs to SSTs.
		var kvs []storage.MVCCKeyValue
		start := storage.MVCCKey{Key: startKey}
		for start.Key != nil {
			var sst []byte
			var summary roachpb.BulkOpSummary
			maxSize := uint64(0)
			prevStart := start
			sstFile := &storage.MemFile{}
			summary, start, err = storage.MVCCExportToSST(ctx, st, e, storage.MVCCExportOptions{
				StartKey:           start,
				EndKey:             endKey,
				StartTS:            startTime,
				EndTS:              endTime,
				ExportAllRevisions: bool(exportAllRevisions),
				TargetSize:         targetSize,
				MaxSize:            maxSize,
				StopMidKey:         bool(stopMidKey),
			}, sstFile)
			require.NoError(t, err)
			sst = sstFile.Data()
			loaded := loadSST(t, sst, startKey, endKey)
			// Ensure that the pagination worked properly.
			if start.Key != nil {
				dataSize := uint64(summary.DataSize)
				require.Truef(t, targetSize <= dataSize, "%d > %d",
					targetSize, summary.DataSize)
				// Now we want to ensure that if we remove the bytes due to the last
				// key that we are below the target size.
				firstKVofLastKey := sort.Search(len(loaded), func(i int) bool {
					return loaded[i].Key.Key.Equal(loaded[len(loaded)-1].Key.Key)
				})
				dataSizeWithoutLastKey := dataSize
				for _, kv := range loaded[firstKVofLastKey:] {
					dataSizeWithoutLastKey -= uint64(len(kv.Key.Key) + len(kv.Value))
				}
				require.Truef(t, targetSize > dataSizeWithoutLastKey, "%d <= %d", targetSize, dataSizeWithoutLastKey)
				// Ensure that maxSize leads to an error if exceeded.
				// Note that this uses a relatively non-sensical value of maxSize which
				// is equal to the targetSize.
				maxSize = targetSize
				dataSizeWhenExceeded := dataSize
				for i := len(loaded) - 1; i >= 0; i-- {
					kv := loaded[i]
					lessThisKey := dataSizeWhenExceeded - uint64(len(kv.Key.Key)+len(kv.Value))
					if lessThisKey >= maxSize {
						dataSizeWhenExceeded = lessThisKey
					} else {
						break
					}
				}
				// It might be the case that this key would lead to an SST of exactly
				// max size, in this case we overwrite max size to be less so that
				// we still generate an error.
				if dataSizeWhenExceeded == maxSize {
					maxSize--
				}
				_, _, err = storage.MVCCExportToSST(ctx, st, e, storage.MVCCExportOptions{
					StartKey:           prevStart,
					EndKey:             endKey,
					StartTS:            startTime,
					EndTS:              endTime,
					ExportAllRevisions: bool(exportAllRevisions),
					TargetSize:         targetSize,
					MaxSize:            maxSize,
					StopMidKey:         false,
				}, &storage.MemFile{})
				require.Regexp(t, fmt.Sprintf("export size \\(%d bytes\\) exceeds max size \\(%d bytes\\)",
					dataSizeWhenExceeded, maxSize), err)
			}
			kvs = append(kvs, loaded...)
		}

		// Compare the output of the current export MVCC to SST logic against the
		// legacy oracle output.
		expectedKVS := loadSST(t, expected, startKey, endKey)
		if len(kvs) != len(expectedKVS) {
			t.Fatalf("got %d kvs but expected %d:\n%v\n%v", len(kvs), len(expectedKVS), kvs, expectedKVS)
		}

		for i := range kvs {
			if !kvs[i].Key.Equal(expectedKVS[i].Key) {
				t.Fatalf("%d key: got %v but expected %v", i, kvs[i].Key, expectedKVS[i].Key)
			}
			if !bytes.Equal(kvs[i].Value, expectedKVS[i].Value) {
				t.Fatalf("%d value: got %x but expected %x", i, kvs[i].Value, expectedKVS[i].Value)
			}
		}
	}
}

func TestRandomKeyAndTimestampExport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storage.DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	mkEngine := func(t *testing.T) (e storage.Engine, cleanup func()) {
		dir, cleanupDir := testutils.TempDir(t)
		e, err := storage.Open(ctx, storage.Filesystem(dir), st, storage.CacheSize(0))
		if err != nil {
			t.Fatal(err)
		}
		return e, func() {
			e.Close()
			cleanupDir()
		}
	}
	const keySize = 100
	const bytesPerValue = 300
	getNumKeys := func(t *testing.T, rnd *rand.Rand, targetSize uint64) (numKeys int) {
		const (
			targetPages = 10
			minNumKeys  = 2 // need > 1 keys for random key test
			maxNumKeys  = 5000
		)
		numKeys = maxNumKeys
		if targetSize > 0 {
			numKeys = rnd.Intn(int(targetSize)*targetPages*2) / bytesPerValue
		}
		if numKeys > maxNumKeys {
			numKeys = maxNumKeys
		} else if numKeys < minNumKeys {
			numKeys = minNumKeys
		}
		return numKeys
	}
	mkData := func(
		t *testing.T, e storage.Engine, rnd *rand.Rand, numKeys int,
	) ([]roachpb.Key, []hlc.Timestamp) {
		// Store generated keys and timestamps.
		var keys []roachpb.Key
		var timestamps []hlc.Timestamp

		numDigits := len(strconv.Itoa(numKeys))
		batch := e.NewBatch()
		for i := 0; i < numKeys; i++ {
			// Ensure walltime and logical are monotonically increasing.
			curWallTime := randutil.RandIntInRange(rnd, 0, math.MaxInt64-1)
			curLogical := randutil.RandIntInRange(rnd, 0, math.MaxInt32-1)
			ts := hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
			timestamps = append(timestamps, ts)

			// Make keys unique and ensure they are monotonically increasing.
			key := roachpb.Key(randutil.RandBytes(rnd, keySize))
			key = append([]byte(fmt.Sprintf("#%0"+strconv.Itoa(numDigits)+"d", i)), key...)
			keys = append(keys, key)

			averageValueSize := bytesPerValue - keySize
			valueSize := randutil.RandIntInRange(rnd, averageValueSize-100, averageValueSize+100)
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, valueSize))
			value.InitChecksum(key)
			if err := storage.MVCCPut(ctx, batch, nil, key, ts, hlc.ClockTimestamp{}, value, nil); err != nil {
				t.Fatal(err)
			}

			// Randomly decide whether to add a newer version of the same key to test
			// MVCC_Filter_All.
			if randutil.RandIntInRange(rnd, 0, math.MaxInt64)%2 == 0 {
				curWallTime++
				ts = hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
				value = roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, 200))
				value.InitChecksum(key)
				if err := storage.MVCCPut(ctx, batch, nil, key, ts, hlc.ClockTimestamp{}, value, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		batch.Close()

		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i].Less(timestamps[j]) })
		return keys, timestamps
	}

	localMax := keys.LocalMax
	testWithTargetSize := func(t *testing.T, targetSize uint64) {
		e, cleanup := mkEngine(t)
		defer cleanup()
		rnd, _ := randutil.NewTestRand()
		numKeys := getNumKeys(t, rnd, targetSize)
		keys, timestamps := mkData(t, e, rnd, numKeys)
		var (
			keyMin = localMax
			keyMax = roachpb.KeyMax

			tsMin = hlc.Timestamp{WallTime: 0, Logical: 0}
			tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
		)

		keyUpperBound := randutil.RandIntInRange(rnd, 1, numKeys)
		keyLowerBound := rnd.Intn(keyUpperBound)
		tsUpperBound := randutil.RandIntInRange(rnd, 1, numKeys)
		tsLowerBound := rnd.Intn(tsUpperBound)

		for _, s := range []struct {
			name   string
			keyMin roachpb.Key
			keyMax roachpb.Key
			tsMin  hlc.Timestamp
			tsMax  hlc.Timestamp
		}{
			{"ts (0-∞]", keyMin, keyMax, tsMin, tsMax},
			{"kv [randLower, randUpper)", keys[keyLowerBound], keys[keyUpperBound], tsMin, tsMax},
			{"kv (randLowerTime, randUpperTime]", keyMin, keyMax, timestamps[tsLowerBound], timestamps[tsUpperBound]},
		} {
			t.Run(fmt.Sprintf("%s, latest", s.name),
				assertEqualKVs(ctx, st, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportLatest, stopAtKeys, targetSize))
			t.Run(fmt.Sprintf("%s, all", s.name),
				assertEqualKVs(ctx, st, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtKeys, targetSize))
			t.Run(fmt.Sprintf("%s, all, split rows", s.name),
				assertEqualKVs(ctx, st, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtTimestamps, targetSize))
		}
	}
	// Exercise min to max time and key ranges.
	for _, targetSize := range []uint64{
		0 /* unlimited */, 1 << 10, 1 << 16, 1 << 20,
	} {
		t.Run(fmt.Sprintf("targetSize=%d", targetSize), func(t *testing.T) {
			testWithTargetSize(t, targetSize)
		})
	}
}
