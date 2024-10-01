// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idxrecommendations

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestIndexRecCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeClusterSettings()

	// Maximum number of unique recommendations the cache will store. Set
	// to 5 to make it easier to hit the limit during testing.
	sqlstats.MaxMemReportedSampleIndexRecommendations.Override(
		context.Background(),
		&st.SV,
		5,
	)
	var cacheUnderTest = NewIndexRecommendationsCache(st)

	datadriven.RunTest(t, "testdata/index-rec-cache", func(t *testing.T, d *datadriven.TestData) string {
		repeat := int64(1)
		var fingerprint string
		var planHash uint64
		var database string
		var stmtType tree.StatementType
		var rec string
		var reset bool
		var internal bool

		d.MaybeScanArgs(t, "fingerprint", &fingerprint)

		ph := ""
		if ok := d.MaybeScanArgs(t, "planHash", &ph); ok {
			pH, err := strconv.ParseUint(ph, 10, 64)
			require.NoError(t, err)
			planHash = pH
		}

		d.MaybeScanArgs(t, "database", &database)

		var stmtTypeStr string
		if ok := d.MaybeScanArgs(t, "stmtType", &stmtTypeStr); ok {
			switch stmtTypeStr {
			case "DML":
				stmtType = tree.TypeDML
			case "DDL":
				stmtType = tree.TypeDDL
			default:
				t.Fatalf("unknown statement type: %s", stmtTypeStr)
			}
		}

		d.MaybeScanArgs(t, "rec", &rec)
		d.MaybeScanArgs(t, "reset", &reset)
		d.MaybeScanArgs(t, "internal", &internal)
		d.MaybeScanArgs(t, "repeat", &repeat)

		switch d.Cmd {
		case "update":
			for i := int64(0); i < repeat; i++ {
				var recs []indexrec.Rec
				if rec != "" {
					rr := strings.Split(rec, ",")
					for _, r := range rr {
						recs = append(recs, indexrec.Rec{
							SQL: r,
						})
					}
				}

				_ = cacheUnderTest.UpdateIndexRecommendations(
					fingerprint,
					planHash,
					database,
					stmtType,
					internal,
					recs,
					rec != "" || reset,
				)
			}
			recInfo, found, cacheSize, _ := cacheUnderTest.getIndexRecommendationAndInfo(
				indexRecKey{
					stmtNoConstants: fingerprint,
					database:        database,
					planHash:        planHash,
				},
			)
			return fmt.Sprintf(
				"recs=%v execCount=%d found=%t cacheSize=%d",
				recInfo.recommendations,
				recInfo.executionCount,
				found,
				cacheSize,
			)
		case "should":
			expectGen := cacheUnderTest.ShouldGenerateIndexRecommendation(
				fingerprint,
				planHash,
				database,
				stmtType,
				internal,
			)
			return fmt.Sprintf("%t\n", expectGen)
		case "set-rec-timeout":
			dur, err := time.ParseDuration(d.Input)
			require.NoError(t, err)
			cacheUnderTest.recTimeoutDuration = dur
		case "set-time-threshold-for-deletion":
			dur, err := time.ParseDuration(d.Input)
			require.NoError(t, err)
			cacheUnderTest.timeThresholdForDeletion = dur
		case "set-time-between-cleanups":
			dur, err := time.ParseDuration(d.Input)
			require.NoError(t, err)
			cacheUnderTest.timeBetweenCleanups = dur
		default:
			t.Fatalf("unknown command: %s", d.Cmd)
		}
		// We test the `should` outputs in the Cmd switch statement above.
		return "ok"
	})
}
