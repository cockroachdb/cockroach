// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/stretchr/testify/require"
)

// TestCheckRowSizeRateLimitsAndTruncates exercises CheckRowSize on a single
// shared RowHelper, calling it directly to avoid the per-statement RowHelper
// creation done by the SQL execution layer. It verifies:
//   - The first violation emits with SkippedLargeRows == 0.
//   - Subsequent rapid violations within largeRowLogEvery are suppressed.
//   - After the window closes, the next emission carries the accumulated
//     SkippedLargeRows count and resets it to zero.
//   - When the raw encoded primary key exceeds maxRawPrimaryKeyLen, the
//     PrimaryKey field in the emitted event carries a truncation marker and
//     stays bounded in size (see keys.PrettyPrintBounded).
func TestCheckRowSizeRateLimitsAndTruncates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Lower the rate-limit interval so the test doesn't have to sleep for a
	// real second between bursts. Restore the default on exit because the
	// limiter is process-global.
	const testInterval = 100 * time.Millisecond
	largeRowLogEvery = log.Every(testInterval)
	defer func() { largeRowLogEvery = log.Every(time.Second) }()

	ctx := context.Background()

	logSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_SQL_EXEC},
		[]string{"large_row"},
		func(entry logpb.Entry) (logpb.Entry, error) {
			entry.Message = entry.Message[entry.StructuredStart:entry.StructuredEnd]
			return entry, nil
		},
	)
	cleanup := log.InterceptWith(ctx, logSpy)
	defer cleanup()

	// Pin to the system tenant so the structured log spy installed in this
	// process actually sees the events. With a probabilistic external-process
	// virtual cluster the SQL execution would happen in a child process whose
	// logs do not flow through our in-process interceptor.
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	codec := srv.SystemLayer().Codec()
	sv := &srv.SystemLayer().ClusterSettings().SV
	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE DATABASE testdb`)
	// A STRING primary key lets us synthesize keys with arbitrary content
	// that PrettyPrint will render verbatim — necessary to drive the
	// PrimaryKey field over maxRawPrimaryKeyLen.
	db.Exec(t, `CREATE TABLE testdb.t (k STRING PRIMARY KEY, v BYTES)`)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "testdb", "t")
	familyID := tableDesc.GetFamilies()[0].ID

	// Build a RowHelper, then override the size thresholds. The defaults
	// pulled from cluster settings (16 MiB log, 80 MiB err) are way above
	// what we want to exercise here.
	sd := &sessiondata.SessionData{}
	rh := NewRowHelper(codec, tableDesc, nil /* indexes */, nil /* uniqueWithTombstoneIndexes */, sd, sv, nil /* metrics */)
	rh.maxRowSizeLog = 1 << 10 // 1 KiB
	rh.maxRowSizeErr = 0       // disabled

	// Each CheckRowSize call uses a key in the table's primary index, with
	// the STRING PK column properly encoded so that PrettyPrint produces
	// realistic output (and we can grow it on demand for the truncation
	// test). The value bytes determine the row size.
	makeKey := func(pk string) roachpb.Key {
		k := keys.SystemSQLCodec.IndexPrefix(uint32(tableDesc.GetID()), uint32(tableDesc.GetPrimaryIndexID()))
		return encoding.EncodeStringAscending(k, pk)
	}
	overThreshold := make([]byte, 2<<10) // 2 KiB > 1 KiB threshold

	skippedRe := regexp.MustCompile(`"SkippedLargeRows":(\d+)`)

	// Helper: extract SkippedLargeRows from a single event's JSON message.
	skippedFor := func(t *testing.T, msg string) uint64 {
		t.Helper()
		match := skippedRe.FindStringSubmatch(msg)
		if len(match) != 2 {
			return 0 // omitted means zero
		}
		v, err := strconv.ParseUint(match[1], 10, 64)
		require.NoError(t, err)
		return v
	}

	t.Run("rate limit and skipped count surface on next emission", func(t *testing.T) {
		logSpy.Reset()
		// Reset the global suppression counter and re-init the limiter so
		// this subtest sees a known starting state regardless of prior
		// activity in the binary.
		skippedLargeRows.Store(0)
		largeRowLogEvery = log.Every(testInterval)

		// First violation: must emit with SkippedLargeRows == 0.
		key0 := makeKey("row-000")
		require.NoError(t, rh.CheckRowSize(ctx, &key0, overThreshold, familyID))

		// Burst of rapid violations: within the rate-limit window, each
		// must be suppressed and counted.
		const burst = 5
		for i := 1; i <= burst; i++ {
			k := makeKey(fmt.Sprintf("row-%03d", i))
			require.NoError(t, rh.CheckRowSize(ctx, &k, overThreshold, familyID))
		}

		// Wait out the rate-limit window. The next emission must carry the
		// accumulated suppressed count.
		time.Sleep(2 * testInterval)
		keyTail := makeKey(fmt.Sprintf("row-%03d", burst+1))
		require.NoError(t, rh.CheckRowSize(ctx, &keyTail, overThreshold, familyID))

		log.FlushAllSync()
		events := logSpy.GetUnreadLogs(logpb.Channel_SQL_EXEC)
		require.Len(t, events, 2,
			"expected exactly two emitted events (initial + post-window); got %d", len(events))
		require.Equal(t, uint64(0), skippedFor(t, events[0].Message),
			"first event should report zero suppressed events")
		require.Equal(t, uint64(burst), skippedFor(t, events[1].Message),
			"post-window event should report the burst of %d suppressed violations", burst)
	})

	t.Run("primary key truncation via SQL INSERT", func(t *testing.T) {
		logSpy.Reset()
		skippedLargeRows.Store(0)
		// Disable rate-limiting entirely for this subtest. The limiter is
		// process-global, so a background internal SQL operation writing a
		// row over the lowered 1 KiB threshold could otherwise consume the
		// window before our INSERT runs, suppressing the event we assert on.
		// This subtest only exercises truncation, not rate-limiting.
		defer TestingDisableLargeRowRateLimit()()

		// 1 KiB log threshold so the row below (8 KiB primary key) crosses it.
		db.Exec(t, "SET CLUSTER SETTING sql.guardrails.max_row_size_log = '1KiB'")
		defer db.Exec(t, "SET CLUSTER SETTING sql.guardrails.max_row_size_log = DEFAULT")

		// 8 KiB > maxRawPrimaryKeyLen (1 KiB), so keys.PrettyPrintBounded
		// slices the raw bytes before PrettyPrint and emits a truncation
		// marker. The decoded STRING value disappears under truncation (the
		// keys decoder emits "???" for the unparseable tail), which is fine
		// — bounding memory is the goal, not preserving column content.
		hugePK := strings.Repeat("k", 8<<10)
		db.Exec(t, "INSERT INTO testdb.t VALUES ($1, $2)", hugePK, []byte("small"))
		log.FlushAllSync()

		events := logSpy.GetUnreadLogs(logpb.Channel_SQL_EXEC)
		require.Len(t, events, 1, "expected exactly one large_row event for the wide INSERT")
		msg := events[0].Message
		require.Contains(t, msg, "(truncated",
			"expected emitted event to contain truncation marker; got %d-byte message", len(msg))
		// The whole JSON event should stay close to the raw-input cap, since
		// the input slice happens before PrettyPrint runs. All other fields
		// are tiny.
		require.Less(t, len(msg), 16<<10,
			"truncated event message is too large: %d bytes", len(msg))
	})
}
