// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttlbench

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/go-openapi/strfmt"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/pflag"
)

// ttlBench measures how long it takes for the row-level TTL job to run on a table.
// See ttlBenchMeta description for details.
type ttlBench struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                 int64
	initialRowCount      int
	rowMessageLength     int
	expiredRowPercentage int
	ttlBatchSize         int
	rangeMinBytes        int
	rangeMaxBytes        int
}

var ttlBenchMeta = workload.Meta{
	Name:        "ttlbench",
	Description: `Measures how long it takes for the row-level TTL job to run on a table.`,
	Details: `

The workload works as follows:
1) Drop TTL table IF EXISTS.
2) Create a table without TTL.
3) Insert initialRowCount number of rows.
4) Gets number of rows that should expire.
5) Wait for table ranges to stabilize after scattering.
6) Enable TTL on table.
7) Poll table until TTL job is complete.
Note: Ops is a no-op and no histograms are used. Benchmarking is done inside Hooks and details are logged.
`,
	Version: "0.0.1",
	New: func() workload.Generator {
		g := &ttlBench{}
		flags := &g.flags
		flags.FlagSet = pflag.NewFlagSet(`ttlbench`, pflag.ContinueOnError)
		flags.Int64Var(&g.seed, `seed`, 1, `Seed for randomization operations.`)
		flags.IntVar(&g.initialRowCount, `initial-row-count`, 0, `Initial rows in table.`)
		flags.IntVar(&g.rowMessageLength, `row-message-length`, 128, `Length of row message.`)
		flags.IntVar(&g.expiredRowPercentage, `expired-row-percentage`, 50, `Percentage of rows that are expired.`)
		flags.IntVar(&g.ttlBatchSize, `ttl-batch-size`, 500, `Size of TTL SELECT and DELETE batches.`)
		flags.IntVar(&g.rangeMinBytes, `range-min-bytes`, 134217728, `Minimum number of bytes in range before merging.`)
		flags.IntVar(&g.rangeMaxBytes, `range-max-bytes`, 536870912, `Maximum number of bytes in range before splitting.`)
		g.connFlags = workload.NewConnFlags(flags)
		return g
	},
}

func init() {
	workload.Register(ttlBenchMeta)
}

const ttlTableName = "ttl_tbl"

func getExpiredRowCount(db *gosql.DB, now time.Time) (int, error) {
	var count int
	row := db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s WHERE expire_at < '%s'", ttlTableName, now.Format(time.RFC3339)))
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func printJobState(ctx context.Context, db *gosql.DB) (retErr error) {
	rows, err := db.Query(`
				SELECT sys_j.id, sys_j.status, crdb_j.started, crdb_j.finished, sys_j.progress
				FROM crdb_internal.jobs AS crdb_j
				JOIN system.jobs as sys_j ON crdb_j.job_id = sys_j.id
				WHERE crdb_j.job_type = 'ROW LEVEL TTL'
				ORDER BY crdb_j.finished DESC
				LIMIT 1;
			`)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
	for rows.Next() {
		var id, status, started, finished string
		var progressBytes []byte
		if err := rows.Scan(&id, &status, &started, &finished, &progressBytes); err != nil {
			return err
		}

		var progress jobspb.Progress
		if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
			return err
		}

		rowLevelTTLProgress := progress.UnwrapDetails().(jobspb.RowLevelTTLProgress)

		log.Infof(
			ctx,
			"job id=%s status=%s started=%s finished=%s\n%s",
			id, status, started, finished, proto.MarshalTextString(&rowLevelTTLProgress),
		)
	}
	return rows.Err()
}

func getLeaseholderToRangeIDsString(leaseholderToRangeIDs map[string][]string) (string, error) {
	sortedLeaseholders := make([]int, len(leaseholderToRangeIDs))
	i := 0
	maxRangeIDLength := 0
	for leaseholder := range leaseholderToRangeIDs {
		leaseholderInt, err := strconv.Atoi(leaseholder)
		if err != nil {
			return "", err
		}
		sortedLeaseholders[i] = leaseholderInt
		i++
		for _, rangeID := range leaseholderToRangeIDs[leaseholder] {
			rangeIDLength := len(rangeID)
			if rangeIDLength > maxRangeIDLength {
				maxRangeIDLength = rangeIDLength
			}
		}
	}
	sort.Ints(sortedLeaseholders)

	var sb strings.Builder
	numLeaseholders := len(sortedLeaseholders)
	numLeaseholdersDigits := len(strconv.Itoa(numLeaseholders))
	leaseholderPadding := strconv.Itoa(numLeaseholdersDigits)
	rangeIDPadding := strconv.Itoa(maxRangeIDLength)
	for _, leaseholder := range sortedLeaseholders {
		leaseholderString := strconv.Itoa(leaseholder)
		sb.WriteString(fmt.Sprintf("leaseholder=%-"+leaseholderPadding+"s rangeIDs=[\n", leaseholderString))
		for i, rangeID := range leaseholderToRangeIDs[leaseholderString] {
			sb.WriteString(fmt.Sprintf("%"+rangeIDPadding+"s ", rangeID))
			const rangesPerLine = 10
			rangeLineIdx := i % rangesPerLine
			// Add newline after rangesPerLine ranges have been printed
			// unless it's the last range because the newline will be printed below.
			if rangeLineIdx == rangesPerLine-1 && rangeLineIdx != len(leaseholderToRangeIDs)-1 {
				sb.WriteString("\n")
			}
		}
		sb.WriteString("\n]\n")
	}
	return sb.String(), nil
}

func waitForDistribution(ctx context.Context, db *gosql.DB) error {

	pollCount := 0
	initialPollAt := timeutil.Now()
	prevPollAt := initialPollAt
	var prevLeaseholderToRangeIDs map[string][]string

	pollFn := func() (poll bool, retErr error) {
		pollAt := timeutil.Now()
		leaseholderToRangeIDs := make(map[string][]string)
		rows, err := db.QueryContext(ctx, fmt.Sprintf(
			`SELECT range_id, lease_holder
			FROM [SHOW RANGES FROM TABLE %s]
			ORDER BY range_id ASC;`,
			ttlTableName,
		))
		if err != nil {
			return false, err
		}
		defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
		for rows.Next() {
			var rangeID, leaseholder string
			if err := rows.Scan(&rangeID, &leaseholder); err != nil {
				return false, err
			}
			leaseholderToRangeIDs[leaseholder] = append(leaseholderToRangeIDs[leaseholder], rangeID)
		}
		if err := rows.Err(); err != nil {
			return false, err
		}

		leaseholderToRangeIDsString, err := getLeaseholderToRangeIDsString(leaseholderToRangeIDs)
		if err != nil {
			return false, err
		}
		log.Infof(
			ctx,
			"pollAt=%s pollCount=%d rangeIDToLeaseholder=\n%s",
			pollAt.Format(strfmt.RFC3339Millis), pollCount, leaseholderToRangeIDsString,
		)

		if !reflect.DeepEqual(prevLeaseholderToRangeIDs, leaseholderToRangeIDs) {
			prevLeaseholderToRangeIDs = leaseholderToRangeIDs
			prevPollAt = pollAt
		} else if pollAt.Sub(prevPollAt) > time.Minute && pollAt.Sub(initialPollAt) > time.Minute {
			return false, nil
		}
		time.Sleep(time.Second)
		pollCount++
		return true, nil
	}

	for {
		poll, err := pollFn()
		if err != nil {
			return err
		}
		if !poll {
			break
		}
	}

	log.Infof(ctx, "waitForDistribution complete pollCount=%d", pollCount)

	return nil
}

func (t *ttlBench) Hooks() workload.Hooks {
	return workload.Hooks{
		PreCreate: func(db *gosql.DB) error {
			// Clear the table in case a previous run left records.
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", ttlTableName)); err != nil {
				return err
			}
			// Configure min/max range bytes.
			_, err := db.Exec(fmt.Sprintf(
				"ALTER DATABASE %s CONFIGURE ZONE USING range_min_bytes = %d, range_max_bytes = %d",
				ttlBenchMeta.Name, t.rangeMinBytes, t.rangeMaxBytes,
			))
			return err
		},
		// The actual benchmarking happens here.
		PostLoad: func(ctx context.Context, db *gosql.DB) error {

			postLoadStartTime := timeutil.Now()
			initialExpiredRowCount, err := getExpiredRowCount(db, postLoadStartTime)
			if err != nil {
				return err
			}

			// Distribute TTL table records across cluster.
			scatterStatement := fmt.Sprintf("ALTER TABLE %s SCATTER", ttlTableName)
			_, err = db.ExecContext(ctx, scatterStatement)
			if err != nil {
				return err
			}

			if err := waitForDistribution(ctx, db); err != nil {
				return err
			}

			// Enable TTL job after records have been inserted.
			ttlStatement := fmt.Sprintf(
				"ALTER TABLE %s SET (ttl_expiration_expression = 'expire_at', ttl_label_metrics = true, ttl_job_cron = '* * * * *', ttl_select_batch_size=%d, ttl_delete_batch_size=%d);",
				ttlTableName, t.ttlBatchSize, t.ttlBatchSize,
			)
			_, err = db.ExecContext(ctx, ttlStatement)
			if err != nil {
				return err
			}
			ttlResetTime := timeutil.Now()
			log.Infof(
				ctx,
				"TTL set initialExpiredRowCount=%d now=%s\n%s",
				initialExpiredRowCount, ttlResetTime.Format(strfmt.RFC3339Millis), ttlStatement,
			)

			pollCount := 0
			expiredRowCount := initialExpiredRowCount
			for {
				expiredRowCount, err = getExpiredRowCount(db, postLoadStartTime)
				if err != nil {
					return err
				}
				pollCount++
				if expiredRowCount < initialExpiredRowCount {
					break
				}
			}

			ttlStartTime := timeutil.Now()
			log.Infof(
				ctx,
				"TTL started pollCount=%d expiredRowCount=%d now=%s",
				pollCount, expiredRowCount, ttlStartTime.Format(strfmt.RFC3339Millis),
			)

			pollCount = 0
			for {
				pollStartTime := timeutil.Now()
				expiredRowCount, err = getExpiredRowCount(db, postLoadStartTime)
				if err != nil {
					return err
				}
				pollCount++
				if expiredRowCount == 0 {
					break
				}

				pollEndTime := timeutil.Now()
				log.Infof(
					ctx,
					"TTL poll pollCount=%d expiredRowCount=%d now=%s",
					pollCount, expiredRowCount, pollEndTime.Format(strfmt.RFC3339Millis),
				)

				pollDuration := pollEndTime.Sub(pollStartTime)
				if pollDuration < time.Second {
					time.Sleep(time.Second - pollDuration)
				}
			}

			ttlEndTime := timeutil.Now()
			log.Infof(
				ctx,
				"TTL complete pollCount=%d duration=%dms now=%s",
				pollCount, ttlEndTime.Sub(ttlStartTime).Milliseconds(), ttlEndTime.Format(strfmt.RFC3339Millis),
			)

			return printJobState(ctx, db)
		},
	}
}

func (t *ttlBench) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	return workload.QueryLoad{}, nil
}

func (t *ttlBench) Meta() workload.Meta {
	return ttlBenchMeta
}

func (t *ttlBench) Tables() []workload.Table {
	initialRowCount := t.initialRowCount
	if initialRowCount < 0 {
		panic(fmt.Sprintf("invalid initial-row-count %d", initialRowCount))
	}
	rowMessageLength := t.rowMessageLength
	if rowMessageLength < 0 {
		panic(fmt.Sprintf("invalid row-message-length %d", rowMessageLength))
	}
	expiredRowPercentage := t.expiredRowPercentage
	if expiredRowPercentage < 0 || expiredRowPercentage > 100 {
		panic(fmt.Sprintf("invalid expired-row-percentage %d", expiredRowPercentage))
	}
	ttlBatchSize := t.ttlBatchSize
	if ttlBatchSize < 0 {
		panic(fmt.Sprintf("invalid ttl-batch-size %d", ttlBatchSize))
	}
	var rowCount atomic.Int64
	return []workload.Table{
		{
			Name: ttlTableName,
			// Start with ttl disabled to prevent job from starting while records are being inserted.
			Schema: `(
	id STRING NOT NULL PRIMARY KEY,
	message TEXT NOT NULL,
  expire_at TIMESTAMPTZ NOT NULL
			)`,
			InitialRows: workload.Tuples(
				initialRowCount,
				func(i int) []interface{} {
					newRowCount := rowCount.Add(1)
					const printRowCount = 100_000
					if newRowCount%printRowCount == 0 {
						log.Infof(context.Background(), "inserted %d rows", newRowCount)
					}
					rng := rand.New(rand.NewSource(t.seed + int64(i)))
					duration := time.Hour * 48
					if expiredRowPercentage > rng.Intn(100) {
						duration *= -1
					}
					return []interface{}{
						uuid.MakeV4().String(), // id
						randutil.RandString(rng, rowMessageLength, randutil.PrintableKeyAlphabet), // message
						timeutil.Now().Add(duration), // expire_at
					}
				},
			),
		},
	}
}

// Flags implements the Flagser interface.
func (t *ttlBench) Flags() workload.Flags { return t.flags }

// ConnFlags implements the ConnFlagser interface.
func (t *ttlBench) ConnFlags() *workload.ConnFlags { return t.connFlags }
