// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/cockroach/pkg/workload/vecann"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// vecIndexOptions defines configuration for a vector index test variant
type vecIndexOptions struct {
	dataset     string        // Dataset name (e.g., "dbpedia-openai-100k-angular")
	nodes       int           // Cluster size
	workers     int           // Concurrent worker goroutines (for Phase 2)
	duration    time.Duration // Phase 2 duration
	prefixCount int           // 0 = no prefix, >0 = use prefix with N categories
	backfillPct int           // Percentage loaded before CREATE INDEX (default 60)
	preBatchSz  int           // Insert batch size pre-index creation
	beamSizes   []int         // Beamsizes to verify with
	minRecall   []float64     // Minimum recall@10 threshold
}

// makeVecIndexTestName generates test name from configuration
func makeVecIndexTestName(opts vecIndexOptions) string {
	baseName := datasetBaseName(opts.dataset)
	return fmt.Sprintf("vecindex/%s/nodes=%d/prefix=%d", baseName, opts.nodes, opts.prefixCount)
}

// datasetBaseName extracts base name from dataset for test naming
func datasetBaseName(dataset string) string {
	// "dbpedia-openai-100k-angular" -> "dbpedia-100k"
	// "random-s-100-euclidean" -> "random-s"
	parts := strings.Split(dataset, "-")
	if len(parts) < 2 {
		return dataset
	}

	// Take meaningful parts, drop metric suffix
	if strings.Contains(dataset, "100k") {
		return parts[0] + "-100k"
	} else if strings.Contains(dataset, "1000k") || strings.Contains(dataset, "1m") {
		return parts[0] + "-1m"
	}
	return parts[0] + "-" + parts[1]
}

// getOperatorForMetric returns SQL distance operator for metric
func getOperatorForMetric(metric vecpb.DistanceMetric) string {
	switch metric {
	case vecpb.CosineDistance:
		return "<=>"
	case vecpb.L2SquaredDistance:
		return "<->"
	case vecpb.InnerProductDistance:
		return "<#>"
	default:
		panic(fmt.Sprintf("unknown metric: %v", metric))
	}
}

// getOpClass returns operator class for CREATE INDEX
func getOpClass(metric vecpb.DistanceMetric) string {
	switch metric {
	case vecpb.CosineDistance:
		return " vector_cosine_ops"
	case vecpb.InnerProductDistance:
		return " vector_ip_ops"
	default:
		return "" // L2 is default
	}
}

func appendCanonicalKey(key []byte, category int, datasetIdx int) []byte {
	key = binary.BigEndian.AppendUint16(key, uint16(category))
	key = binary.BigEndian.AppendUint32(key, uint32(datasetIdx))
	return key
}

// makeCanonicalKey generates 4-byte primary key from dataset index
func makeCanonicalKey(category int, datasetIdx int) []byte {
	key := make([]byte, 0, 6)
	return appendCanonicalKey(key, category, datasetIdx)
}

// backfillState represents the current state of index backfill
type backfillState int32

const (
	statePreBackfill      backfillState = iota // Loading unindexed data
	stateIndexCreating                         // CREATE INDEX issued, job not visible yet
	stateBackfillRunning                       // Backfill job visible and running
	stateBackfillComplete                      // Backfill done, still loading canonical rows
	stateSteadyState                           // All done (used in Phase 2)
)

func (s backfillState) String() string {
	switch s {
	case statePreBackfill:
		return "pre-backfill"
	case stateIndexCreating:
		return "index-creating"
	case stateBackfillRunning:
		return "backfill-running"
	case stateBackfillComplete:
		return "backfill-complete"
	case stateSteadyState:
		return "steady-state"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// phaseString returns the inserted_phase value for the current state
func (s backfillState) phaseString() string {
	switch s {
	case statePreBackfill:
		return "initial"
	case stateIndexCreating, stateBackfillRunning:
		return "during-backfill"
	case stateBackfillComplete:
		return "post-backfill-canonical"
	case stateSteadyState:
		return "post-backfill"
	default:
		return "unknown"
	}
}

func registerVectorIndex(r registry.Registry) {
	configs := []vecIndexOptions{
		// Standard - no prefix
		{
			dataset:     "dbpedia-openai-100k-angular",
			nodes:       3,
			workers:     16,
			duration:    60 * time.Minute,
			prefixCount: 0,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{8, 16, 32, 64, 128},
			minRecall:   []float64{0.80, 0.80, 0.85, 0.90, 0.90},
		},
		// Local - no prefix
		{
			dataset:     "random-s-100-euclidean",
			nodes:       1,
			workers:     4,
			duration:    30 * time.Minute,
			prefixCount: 0,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{16, 32, 64},
			minRecall:   []float64{0.95, 0.95, 0.95},
		},
		// Large - no prefix
		{
			dataset:     "dbpedia-openai-1000k-angular",
			nodes:       6,
			workers:     64,
			duration:    4 * time.Hour,
			prefixCount: 0,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{8, 16, 32, 64, 128},
			minRecall:   []float64{0.80, 0.80, 0.85, 0.90, 0.90},
		},
		// Standard - with prefix
		{
			dataset:     "dbpedia-openai-100k-angular",
			nodes:       3,
			workers:     16,
			duration:    60 * time.Minute,
			prefixCount: 3,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{8, 16, 32, 64, 128},
			minRecall:   []float64{0.80, 0.80, 0.85, 0.90, 0.90},
		},
		// Local - with prefix
		{
			dataset:     "random-s-100-euclidean",
			nodes:       1,
			workers:     4,
			duration:    30 * time.Minute,
			prefixCount: 2,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{16, 32, 64},
			minRecall:   []float64{0.95, 0.95, 0.95},
		},
	}

	for _, opts := range configs {
		opts := opts // capture loop variable

		name := makeVecIndexTestName(opts)

		r.Add(registry.TestSpec{
			Name:             name,
			Owner:            registry.OwnerSQLQueries,
			Cluster:          r.MakeClusterSpec(opts.nodes),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runVectorIndex(ctx, t, c, opts)
			},
		})
	}
}

func runVectorIndex(ctx context.Context, t test.Test, c cluster.Cluster, opts vecIndexOptions) {
	// Load dataset metadata
	t.L().Printf("Loading dataset %s", opts.dataset)
	loader := vecann.DatasetLoader{
		DatasetName: opts.dataset,
		OnProgress: func(ctx context.Context, format string, args ...any) {
			t.L().Printf(format, args...)
		},
	}
	require.NoError(t, loader.Load(ctx))

	// Derive distance metric
	metric, err := vecann.DeriveDistanceMetric(opts.dataset)
	require.NoError(t, err)

	t.L().Printf("Dataset ready: %d train vectors, %d test vectors, %d dims, metric=%v",
		loader.Data.TrainCount, loader.Data.Test.Count, loader.Data.Test.Dims, metric)

	// Start cluster
	t.L().Printf("Starting cluster with %d nodes", opts.nodes)
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule),
		install.MakeClusterSettings(), c.All())

	// Create connection pool for workers
	urls, err := c.ExternalPGUrl(ctx, t.L(), c.CRDBNodes(), roachprod.PGURLOptions{})
	require.NoError(t, err)
	config, err := pgxpool.ParseConfig(urls[rand.Intn(len(urls))])
	require.NoError(t, err)

	config.MaxConns = int32(opts.workers)
	config.MinConns = 1
	config.MaxConnLifetime = opts.duration + 1*time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()
	require.NoError(t, pool.Ping(ctx))

	t.L().Printf("Creating schema and loading data")
	testBackfillAndMerge(ctx, t, c, pool, &loader.Data, &opts, metric)

	t.L().Printf("Testing recall of loaded data")
	testRecall(ctx, t, pool, &loader.Data, &opts, metric)
}

func testBackfillAndMerge(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	pool *pgxpool.Pool,
	data *vecann.Dataset,
	opts *vecIndexOptions,
	metric vecpb.DistanceMetric,
) {
	batchSize := opts.preBatchSz
	numCategories := max(opts.prefixCount, 1)

	db, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE vecindex_test (
				id BYTES,
				category INT NOT NULL,
				embedding VECTOR(%d) NOT NULL,
				inserted_phase TEXT NOT NULL,
				worker_id INT NOT NULL,
				excluded BOOL DEFAULT false,
				INDEX (excluded),
				PRIMARY KEY (id)
			)`, data.Dims))
	require.NoError(t, err)
	t.L().Printf("Table vecindex_test created")

	// Shared state for workers
	var state int32 = int32(statePreBackfill)
	var rowsInserted int32
	blockBackfill := make(chan struct{})
	createIndexStartThresh := (data.TrainCount * opts.backfillPct) / 100

	ci := t.NewGroup()

	// Start a goroutine to create a vector index when we're signaled by one of the workers
	ci.Go(func(ctx context.Context, l *logger.Logger) error {
		// Wait until a worker signals us to start the CREATE INDEX
		<-blockBackfill

		atomic.StoreInt32(&state, int32(stateIndexCreating))
		l.Printf("Executing CREATE VECTOR INDEX at %d rows", atomic.LoadInt32(&rowsInserted))

		startCreateIndex := timeutil.Now()
		opClass := getOpClass(metric)
		var indexSQL string
		if opts.prefixCount > 0 {
			indexSQL = fmt.Sprintf("CREATE VECTOR INDEX vecidx ON vecindex_test (category, embedding%s)", opClass)
		} else {
			indexSQL = fmt.Sprintf("CREATE VECTOR INDEX vecidx ON vecindex_test (embedding%s)", opClass)
		}

		_, err := db.ExecContext(ctx, indexSQL)
		if err != nil {
			return errors.Wrapf(err, "Failed to create vector index")
		} else {
			dur := timeutil.Since(startCreateIndex).Truncate(time.Second)
			rate := float64(data.TrainCount) / dur.Seconds()
			l.Printf("CREATE VECTOR INDEX completed in %v (%.1f rows per second)", dur, rate)
		}
		return nil
	})

	t.L().Printf(
		"Loading %d rows into %d categories (%d rows total)",
		data.TrainCount,
		numCategories,
		data.TrainCount*numCategories,
	)

	var fileStart int
	loadStart := timeutil.Now()
	// Iterate through the data files in the data set
	for {
		filename := data.GetNextTrainFile()
		hasMore, err := data.Next()
		require.NoError(t, err)
		if !hasMore {
			dur := timeutil.Since(loadStart).Truncate(time.Second)
			rate := float64(data.TrainCount) / dur.Seconds()
			t.L().Printf("Data loaded in %v (%.1f rows per second)", dur, rate)
			break
		}
		t.L().Printf("Loading data file: %s", filename)

		// Create workers to load this data file and dispatch part of the file to each of them.
		m := t.NewGroup()
		countPerProc := (data.Train.Count / opts.workers) + 1
		for worker := range opts.workers {
			start := worker * countPerProc
			end := min(start+countPerProc, data.Train.Count)
			m.Go(func(ctx context.Context, l *logger.Logger) error {
				conn, err := pool.Acquire(ctx)
				require.NoError(t, err)
				defer conn.Release()

				for j := start; j < end; j += batchSize {
					sz := int32(batchSize)
					ri := int(atomic.AddInt32(&rowsInserted, sz))
					phaseStr := backfillState(atomic.LoadInt32(&state)).phaseString()
					vectors := data.Train.Slice(j, min(j+batchSize, end)-j)
					err := insertVectors(ctx, conn, worker, numCategories, fileStart+j, phaseStr, vectors, false)
					if err != nil {
						return err
					}
					// If this is the batch that spanned the create index start threshold, signal the creator.
					if ri > createIndexStartThresh && ri <= createIndexStartThresh+batchSize {
						close(blockBackfill)
					}
				}
				return nil
			})
		}
		// Wait for this batch of loaders
		m.Wait()
		fileStart += data.Train.Count
	}

	// Wait for create index to finish
	ci.Wait()
}

func testRecall(
	ctx context.Context,
	t test.Test,
	pool *pgxpool.Pool,
	data *vecann.Dataset,
	opts *vecIndexOptions,
	metric vecpb.DistanceMetric,
) {
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	maxResults := 10
	operator := getOperatorForMetric(metric)

	var categories int
	var searchSQL string
	var args []any
	var hasPrefix bool
	if opts.prefixCount > 0 {
		categories = opts.prefixCount
		searchSQL = fmt.Sprintf(
			"SELECT id FROM vecindex_test@vecidx WHERE category = $1 "+
				"ORDER BY embedding %s $2 LIMIT %d", operator, maxResults)
		args = make([]any, 2)
		hasPrefix = true
	} else {
		categories = 1
		searchSQL = fmt.Sprintf(
			"SELECT id FROM vecindex_test@vecidx ORDER BY embedding %s $1 LIMIT %d", operator, maxResults)
		args = make([]any, 1)
	}

	results := make([]cspann.KeyBytes, maxResults)
	recalls := make([]float64, categories)
	primaryKeys := make([]byte, maxResults*6)
	truth := make([]cspann.KeyBytes, maxResults)

	for i, beamSize := range opts.beamSizes {
		recalls = recalls[:0]
		minRecall := opts.minRecall[i]
		_, err = conn.Exec(ctx, fmt.Sprintf("SET vector_search_beam_size = %d", beamSize))
		require.NoError(t, err)

		for cat := range categories {
			var sumRecall float64
			for j := range data.Test.Count {
				queryVec := data.Test.At(j)
				args = args[:0]
				if hasPrefix {
					args = append(args, cat)
				}
				args = append(args, queryVec)

				rows, err := conn.Query(ctx, searchSQL, args...)
				require.NoError(t, err)

				results = results[:0]
				for rows.Next() {
					var id []byte
					err = rows.Scan(&id)
					require.NoError(t, err)
					results = append(results, id)
				}
				require.NoError(t, rows.Err())

				primaryKeys = primaryKeys[:0]
				truth = truth[:0]
				for n := range maxResults {
					primaryKeys = appendCanonicalKey(primaryKeys, cat, int(data.Neighbors[j][n]))
					truth = append(truth, primaryKeys[len(primaryKeys)-6:])
				}

				sumRecall += vecann.CalculateRecall(results, truth)
			}
			avgRecall := sumRecall / float64(data.Test.Count)
			require.GreaterOrEqual(t, avgRecall, minRecall)
			recalls = append(recalls, avgRecall)
		}
		t.L().Printf("beam size=%d : %v", beamSize, recalls)
	}
}

func insertVectors(
	ctx context.Context,
	conn *pgxpool.Conn,
	workerID int,
	numCats int,
	startIdx int,
	phaseStr string,
	vectors vector.Set,
	excluded bool,
) error {
	args := make([]any, vectors.Count*numCats*5)
	var queryBuilder strings.Builder
	queryBuilder.Grow(100 + vectors.Count*numCats*34)
	queryBuilder.WriteString("INSERT INTO vecindex_test " +
		"(id, category, embedding, inserted_phase, worker_id, excluded) VALUES ")
	rowNum := 0
	for i := range vectors.Count {
		for cat := range numCats {
			if rowNum > 0 {
				queryBuilder.WriteString(", ")
			}
			j := rowNum * 5
			fmt.Fprintf(&queryBuilder, "($%d, $%d, $%d, $%d, $%d, %v)", j+1, j+2, j+3, j+4, j+5, excluded)
			args[j] = makeCanonicalKey(cat, startIdx+i)
			args[j+1] = cat
			args[j+2] = vectors.At(i)
			args[j+3] = phaseStr
			args[j+4] = workerID
			rowNum++
		}
	}
	query := queryBuilder.String()

	for {
		_, err := conn.Exec(ctx, query, args...)

		var pgErr *pgconn.PgError
		if err != nil && errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40001", "40P01":
				continue
			}
		}

		return errors.Wrapf(err, "Failed to run: %s %v", query, args)
	}
}
