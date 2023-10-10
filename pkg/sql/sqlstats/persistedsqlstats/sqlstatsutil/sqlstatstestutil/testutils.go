// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatstestutil

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// GetRandomizedCollectedStatementStatisticsForTest returns a
// appstatspb.CollectedStatementStatistics with its fields randomly filled.
func GetRandomizedCollectedStatementStatisticsForTest(
	t *testing.T,
) (result appstatspb.CollectedStatementStatistics) {
	data := genRandomData()
	fillObject(t, reflect.ValueOf(&result), &data)

	return result
}

// GetRandomizedCollectedTransactionStatisticsForTest returns a
// appstatspb.CollectedStatementStatistics with its fields randomly filled.
func GetRandomizedCollectedTransactionStatisticsForTest(
	t *testing.T,
) (result appstatspb.CollectedTransactionStatistics) {
	data := genRandomData()
	fillObject(t, reflect.ValueOf(&result), &data)

	return result
}

type randomData struct {
	Bool        bool
	String      string
	Int64       int64
	Float       float64
	IntArray    []int64
	StringArray []string
	Time        time.Time
}

var alphabet = []rune("abcdefghijklmkopqrstuvwxyz")

func genRandomData() randomData {
	r := randomData{}
	r.Bool = rand.Float64() > 0.5

	// Randomly generating 20-character string.
	b := strings.Builder{}
	for i := 0; i < 20; i++ {
		b.WriteRune(alphabet[rand.Intn(26)])
	}
	r.String = b.String()

	// Generate a randomized array of length 5.
	arrLen := 5
	r.StringArray = make([]string, arrLen)
	for i := 0; i < arrLen; i++ {
		// Randomly generating 10-character string.
		b := strings.Builder{}
		for j := 0; j < 10; j++ {
			b.WriteRune(alphabet[rand.Intn(26)])
		}
		r.StringArray[i] = b.String()
	}

	r.Int64 = rand.Int63()
	r.Float = rand.Float64()

	// Generate a randomized array of length 5.
	r.IntArray = make([]int64, arrLen)
	for i := 0; i < arrLen; i++ {
		r.IntArray[i] = rand.Int63()
	}

	r.Time = timeutil.Now()
	return r
}

func fillTemplate(t *testing.T, tmplStr string, data randomData) string {
	joinInts := func(arr []int64) string {
		strArr := make([]string, len(arr))
		for i, val := range arr {
			strArr[i] = strconv.FormatInt(val, 10)
		}
		return strings.Join(strArr, ",")
	}
	joinStrings := func(arr []string) string {
		strArr := make([]string, len(arr))
		for i, val := range arr {
			strArr[i] = fmt.Sprintf("%q", val)
		}
		return strings.Join(strArr, ",")
	}
	stringifyTime := func(tm time.Time) string {
		s, err := tm.MarshalText()
		require.NoError(t, err)
		return string(s)
	}
	tmpl, err := template.
		New("").
		Funcs(template.FuncMap{
			"joinInts":      joinInts,
			"joinStrings":   joinStrings,
			"stringifyTime": stringifyTime,
		}).
		Parse(tmplStr)
	require.NoError(t, err)

	b := strings.Builder{}
	err = tmpl.Execute(&b, data)
	require.NoError(t, err)

	return b.String()
}

// fieldBlacklist contains a list of fields in the protobuf message where
// we don't populate using random data. This can be because it is either a
// complex type or might be the test data is already hard coded with values.
var fieldBlacklist = map[string]struct{}{
	"App":                     {},
	"SensitiveInfo":           {},
	"LegacyLastErr":           {},
	"LegacyLastErrRedacted":   {},
	"StatementFingerprintIDs": {},
	"AggregatedTs":            {},
	"AggregationInterval":     {},
}

func fillObject(t *testing.T, val reflect.Value, data *randomData) {
	// Do not set the fields that are not being encoded as json.
	if val.Kind() != reflect.Ptr {
		t.Fatal("not a pointer type")
	}

	val = reflect.Indirect(val)

	switch val.Kind() {
	case reflect.Uint64:
		val.SetUint(uint64(0))
	case reflect.Int64:
		val.SetInt(data.Int64)
	case reflect.String:
		val.SetString(data.String)
	case reflect.Float64:
		val.SetFloat(data.Float)
	case reflect.Bool:
		val.SetBool(data.Bool)
	case reflect.Slice:
		switch val.Type().String() {
		case "[]string":
			for _, randString := range data.StringArray {
				val.Set(reflect.Append(val, reflect.ValueOf(randString)))
			}
		case "[]int64":
			for _, randInt := range data.IntArray {
				val.Set(reflect.Append(val, reflect.ValueOf(randInt)))
			}
		}
	case reflect.Struct:
		switch val.Type().Name() {
		// Special handling time.Time.
		case "Time":
			val.Set(reflect.ValueOf(data.Time))
			return
		default:
			numFields := val.NumField()
			for i := 0; i < numFields; i++ {
				fieldName := val.Type().Field(i).Name
				fieldAddr := val.Field(i).Addr()
				if _, ok := fieldBlacklist[fieldName]; ok {
					continue
				}

				fillObject(t, fieldAddr, data)
			}
		}
	default:
		t.Fatalf("unsupported type: %s", val.Kind().String())
	}
}

func InsertMockedIntoSystemStmtStats(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stmtStats *appstatspb.CollectedStatementStatistics,
	nodeID base.SQLInstanceID,
	aggInterval *time.Duration,
) error {
	if stmtStats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	stmtFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.ID))
	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.Key.TransactionFingerprintID))
	planHash := sqlstatsutil.EncodeUint64ToBytes(stmtStats.Key.PlanHash)

	metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stmtStats)
	if err != nil {
		return err
	}

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stmtStats.Stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stmtStats.Stats.SensitiveInfo.MostRecentPlanDescription))

	metadata := tree.NewDJSON(metadataJSON)

	_, err = ie.ExecEx(ctx, "insert-mock-stmt-stats", nil, sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.statement_statistics
VALUES ($1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		stmtStats.AggregatedTs, // aggregated_ts
		stmtFingerprint,        // fingerprint_id
		txnFingerprint,         // transaction_fingerprint_id
		planHash,               // plan_hash
		stmtStats.Key.App,      // app_name
		nodeID,                 // node_id
		aggIntervalVal,         // agg_interval
		metadata,               // metadata
		statistics,             // statistics
		plan,                   // plan
	)

	return err
}

func InsertMockedIntoSystemTxnStats(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stats *appstatspb.CollectedTransactionStatistics,
	nodeID base.SQLInstanceID,
	aggInterval *time.Duration,
) error {
	if stats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stats)
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)
	aggregatedTs := stats.AggregatedTs

	_, err = ie.ExecEx(ctx, "insert-mock-txn-stats", nil, sessiondata.NodeUserSessionDataOverride,
		` UPSERT INTO system.transaction_statistics
VALUES ($1 ,$2, $3, $4, $5, $6, $7)`,
		aggregatedTs,   // aggregated_ts
		txnFingerprint, // fingerprint_id
		stats.App,      // app_name
		nodeID,         // node_id
		aggIntervalVal, // agg_interval
		metadata,       // metadata
		statistics,     // statistics
	)

	return err
}

func InsertMockedIntoSystemStmtActivity(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stmtStats *appstatspb.CollectedStatementStatistics,
	aggInterval *time.Duration,
) error {
	if stmtStats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	stmtFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.ID))
	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.Key.TransactionFingerprintID))
	planHash := sqlstatsutil.EncodeUint64ToBytes(stmtStats.Key.PlanHash)

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stmtStats.Stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stmtStats.Stats.SensitiveInfo.MostRecentPlanDescription))

	metadataJSON, err := sqlstatsutil.BuildStmtDetailsMetadataJSON(
		&appstatspb.AggregatedStatementMetadata{
			Query:          stmtStats.Key.Query,
			FormattedQuery: "",
			QuerySummary:   "",
			StmtType:       "",
			AppNames:       []string{stmtStats.Key.App},
			Databases:      []string{stmtStats.Key.Database},
			ImplicitTxn:    false,
			DistSQLCount:   0,
			FailedCount:    0,
			FullScanCount:  0,
			VecCount:       0,
			TotalCount:     0,
		})
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)

	_, err = ie.ExecEx(ctx, "insert-mock-stmt-activity", nil, sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.statement_activity
VALUES ($1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		stmtStats.AggregatedTs, // aggregated_ts
		stmtFingerprint,        // fingerprint_id
		txnFingerprint,         // transaction_fingerprint_id
		planHash,               // plan_hash
		stmtStats.Key.App,      // app_name
		aggIntervalVal,         // agg_interval
		metadata,               // metadata
		statistics,             // statistics
		plan,                   // plan
		// TODO allow these values to be mocked. No need ffor them for now.
		[]string{}, // index_recommendations
		1,          // execution_count
		1,          // execution_total_seconds
		1,          // execution_total_cluster_seconds
		1,          // contention_time_avg_seconds
		1,          // cpu_sql_avg_nanos
		1,          //  service_latency_avg_seconds
		1,          // service_latency_p99_seconds
	)

	return err
}

func InsertMockedIntoSystemTxnActivity(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stats *appstatspb.CollectedTransactionStatistics,
	aggInterval *time.Duration,
) error {
	if stats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stats)
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)
	aggregatedTs := stats.AggregatedTs

	_, err = ie.ExecEx(ctx, "insert-mock-txn-activity", nil, sessiondata.NodeUserSessionDataOverride,
		` UPSERT INTO system.transaction_activity
VALUES ($1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		aggregatedTs,   // aggregated_ts
		txnFingerprint, // fingerprint_id
		stats.App,      // app_name
		aggIntervalVal, // agg_interval
		metadata,       // metadata
		statistics,     // statistics
		// TODO (xinhaoz) allow mocking of these fields. Not necessary at the moment.
		"", // query
		1,  // execution_count
		1,  // execution_total_seconds
		1,  // execution_total_cluster_seconds
		1,  // contention_time_avg_seconds
		1,  // cpu_sql_avg_nanos
		1,  // service_latency_avg_seconds
		1,  // service_latency_p99_seconds
	)

	return err
}
