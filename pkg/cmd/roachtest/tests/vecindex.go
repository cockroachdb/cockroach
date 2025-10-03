// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/workload/vecann"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// vecIndexOptions defines configuration for a vector index test variant
type vecIndexOptions struct {
	dataset     string        // Dataset name (e.g., "dbpedia-openai-100k-angular")
	nodes       int           // Cluster size
	workers     int           // Concurrent worker goroutines (for Phase 2)
	duration    time.Duration // Phase 2 duration
	timeout     time.Duration // Test timeout
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

func registerVectorIndex(r registry.Registry) {
	configs := []vecIndexOptions{
		// Standard - no prefix
		{
			dataset:     "dbpedia-openai-100k-angular",
			nodes:       3,
			workers:     16,
			duration:    60 * time.Minute,
			timeout:     2 * time.Hour,
			prefixCount: 0,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{8, 16, 32, 64, 128},
			minRecall:   []float64{0.76, 0.83, 0.88, 0.92, 0.94},
		},
		// Local - no prefix
		{
			dataset:     "random-s-100-euclidean",
			nodes:       1,
			workers:     4,
			duration:    30 * time.Minute,
			timeout:     time.Hour,
			prefixCount: 0,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{16, 32, 64},
			minRecall:   []float64{0.94, 0.95, 0.95},
		},
		// Large - no prefix
		{
			dataset:     "dbpedia-openai-1000k-angular",
			nodes:       6,
			workers:     64,
			duration:    4 * time.Hour,
			timeout:     16 * time.Hour,
			prefixCount: 0,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{8, 16, 32, 64, 128},
			minRecall:   []float64{0.64, 0.74, 0.81, 0.87, 0.90},
		},
		// Standard - with prefix
		{
			dataset:     "dbpedia-openai-100k-angular",
			nodes:       3,
			workers:     16,
			duration:    60 * time.Minute,
			timeout:     4 * time.Hour,
			prefixCount: 3,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{8, 16, 32, 64, 128},
			minRecall:   []float64{0.76, 0.83, 0.88, 0.92, 0.94},
		},
		// Local - with prefix
		{
			dataset:     "random-s-100-euclidean",
			nodes:       1,
			workers:     4,
			duration:    30 * time.Minute,
			timeout:     2 * time.Hour,
			prefixCount: 2,
			backfillPct: 60,
			preBatchSz:  100,
			beamSizes:   []int{16, 32, 64},
			minRecall:   []float64{0.94, 0.95, 0.95},
		},
	}

	for _, opts := range configs {
		opts := opts // capture loop variable

		name := makeVecIndexTestName(opts)

		r.Add(registry.TestSpec{
			Name:             name,
			Owner:            registry.OwnerSQLQueries,
			Timeout:          opts.timeout,
			Cluster:          r.MakeClusterSpec(opts.nodes),
			CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
			Suites:           registry.Suites(registry.VecIndex),
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

	t.L().Printf("=== Test completed successfully ===")
}
