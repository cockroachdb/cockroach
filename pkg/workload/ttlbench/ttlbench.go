// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttlbench

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/go-openapi/strfmt"
	"github.com/spf13/pflag"
)

// ttlBench measures how long it takes for the ttl job to run
type ttlBench struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                 int64
	initialRowCount      int
	rowMessageLength     int
	expiredRowPercentage int
	ttlBatchSize         int
	ttlRangeConcurrency  int
}

var ttlBenchMeta = workload.Meta{
	Name:         "ttlbench",
	Description:  "Measures how quickly the TTL job runs on a table",
	Version:      "0.0.1",
	PublicFacing: false,
	New: func() workload.Generator {
		g := &ttlBench{}
		g.flags.FlagSet = pflag.NewFlagSet(`ttlbench`, pflag.ContinueOnError)
		g.flags.Int64Var(&g.seed, `seed`, 1, `seed for randomization operations`)
		g.flags.IntVar(&g.initialRowCount, `initial-row-count`, 0, `initial rows in table`)
		g.flags.IntVar(&g.rowMessageLength, `row-message-length`, 128, `length of row message`)
		g.flags.IntVar(&g.expiredRowPercentage, `expired-row-percentage`, 50, `percentage of rows that are expired`)
		g.flags.IntVar(&g.ttlBatchSize, `ttl-batch-size`, 500, `size of TTL SELECT and DELETE batches`)
		g.flags.IntVar(&g.ttlRangeConcurrency, `ttl-range-concurrency`, 1, `number of concurrent ranges to process per node`)

		g.connFlags = workload.NewConnFlags(&g.flags)
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
	if err := row.Err(); err != nil {
		return 0, err
	}
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (t *ttlBench) Hooks() workload.Hooks {
	return workload.Hooks{
		// clear the table in case a previous run left records
		PreCreate: func(db *gosql.DB) error {
			_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", ttlTableName))
			return err
		},
		// the actual benchmarking happens here
		PostLoad: func(ctx context.Context, db *gosql.DB) error {

			postLoadStartTime := timeutil.Now()
			initialExpiredRowCount, err := getExpiredRowCount(db, postLoadStartTime)
			if err != nil {
				return err
			}

			// enable ttl job after records have been inserted
			_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s RESET (ttl_pause)", ttlTableName))
			if err != nil {
				return err
			}
			ttlResetTime := timeutil.Now()
			log.Infof(ctx, "TTL reset initialExpiredRowCount=%d now=%s", initialExpiredRowCount, ttlResetTime.Format(strfmt.RFC3339Millis))

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
			log.Infof(ctx, "TTL started pollCount=%d expiredRowCount=%d now=%s", pollCount, expiredRowCount, ttlStartTime.Format(strfmt.RFC3339Millis))

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
				log.Infof(ctx, "TTL poll pollCount=%d expiredRowCount=%d now=%s", pollCount, expiredRowCount, pollEndTime.Format(strfmt.RFC3339Millis))

				pollDuration := pollEndTime.Sub(pollStartTime)
				if pollDuration < time.Second {
					time.Sleep(time.Second - pollDuration)
				}
			}

			ttlEndTime := timeutil.Now()
			log.Infof(ctx, "TTL complete pollCount=%d duration=%dms now=%s", pollCount, ttlStartTime.Sub(ttlEndTime).Milliseconds(), ttlEndTime.Format(strfmt.RFC3339Millis))

			return nil
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
	ttlRangeConcurrency := t.ttlRangeConcurrency
	if ttlRangeConcurrency < 0 {
		panic(fmt.Sprintf("invalid ttl-range-concurrency %d", ttlRangeConcurrency))
	}
	return []workload.Table{
		{
			Name: ttlTableName,
			// start with ttl disabled to prevent job from starting while records are being added
			Schema: fmt.Sprintf(`(
	id STRING NOT NULL PRIMARY KEY,
	message TEXT NOT NULL,
  expire_at TIMESTAMPTZ NOT NULL
) WITH (ttl_expiration_expression = 'expire_at', ttl_label_metrics = true, ttl_job_cron = '* * * * *', ttl_pause = 'true', ttl_select_batch_size=%d, ttl_delete_batch_size=%d, ttl_range_concurrency=%d)`,
				ttlBatchSize,
				ttlBatchSize,
				ttlRangeConcurrency,
			),
			InitialRows: workload.Tuples(
				initialRowCount,
				func(i int) []interface{} {
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

func (t *ttlBench) Flags() workload.Flags {
	return t.flags
}
