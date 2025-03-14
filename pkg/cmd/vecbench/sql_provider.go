// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLProvider implements VectorProvider using a SQL database connection to a
// CockroachDB instance.
type SQLProvider struct {
	datasetName string
	dims        int
	options     cspann.IndexOptions
	pool        *pgxpool.Pool
	tableName   string
	retryCount  atomic.Uint64
}

// NewSQLProvider creates a new SQLProvider that connects to a CockroachDB
// instance.
func NewSQLProvider(
	ctx context.Context, datasetName string, dims int, options cspann.IndexOptions,
) (*SQLProvider, error) {
	// Create connection pool.
	config, err := pgxpool.ParseConfig(*flagDBConnStr)
	if err != nil {
		return nil, errors.Wrap(err, "parsing connection string")
	}

	// Set reasonable defaults for the connection pool.
	config.MaxConns = int32(runtime.GOMAXPROCS(-1))
	config.MinConns = 1
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "creating connection pool")
	}

	// Verify connection works.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "connecting to database")
	}

	// Create sanitized table and index names.
	tableName := fmt.Sprintf("vecbench_%s", sanitizeIdentifier(datasetName))

	return &SQLProvider{
		datasetName: datasetName,
		dims:        dims,
		options:     options,
		pool:        pool,
		tableName:   tableName,
	}, nil
}

// Close implements the VectorProvider interface.
func (s *SQLProvider) Close() {
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
}

// Load implements the VectorProvider interface.
func (s *SQLProvider) Load(ctx context.Context) (bool, error) {
	// Assume that if the table exists, that it contains the dataset rows.
	// TODO(andyk): Could write a status row to a table when building it, so that
	// we know that it isn't truncated in some way.
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_name = $1
		)`, s.tableName).Scan(&exists)

	if err != nil {
		return false, errors.Wrap(err, "checking if table exists")
	}
	return exists, nil
}

// Save implements the VectorProvider interface.
func (s *SQLProvider) Save(ctx context.Context) error {
	// For SQL provider, data is already saved in the database, so nothing to do.
	return nil
}

// Clear implements the VectorProvider interface.
func (s *SQLProvider) Clear(ctx context.Context) error {
	// Drop the table if it exists.
	_, err := s.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", s.tableName))
	return errors.Wrap(err, "dropping table")
}

// InsertVectors implements the VectorProvider interface.
func (s *SQLProvider) InsertVectors(
	ctx context.Context, keys []cspann.KeyBytes, vectors vector.Set,
) error {
	// Create the table if it doesn't exist.
	if err := s.ensureTableExists(ctx); err != nil {
		return err
	}

	// Retry loop.
	for {
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			// Prepare batch insert.
			batch := &pgx.Batch{}

			// Insert vectors in batches.
			for i := 0; i < vectors.Count; i++ {
				batch.Queue(fmt.Sprintf(
					"INSERT INTO %s (id, embedding) VALUES ($1, $2)",
					s.tableName),
					keys[i],
					vectors.At(i),
				)
			}

			br := tx.SendBatch(ctx, batch)
			if err := br.Close(); err != nil {
				return errors.Wrap(err, "closing batch")
			}
			return nil
		})

		var pgErr *pgconn.PgError
		if err != nil && errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40001", "40P01":
				// Retry on serialization failure or deadlock detected.
				s.retryCount.Add(1)
				continue
			}
		}

		return err
	}
}

// Search implements the VectorProvider interface.
func (s *SQLProvider) Search(
	ctx context.Context, vec vector.T, maxResults int, beamSize int, stats *cspann.SearchStats,
) ([]cspann.KeyBytes, error) {
	// Construct a query that searches for similar vectors.
	query := fmt.Sprintf(`
		SELECT id
		FROM %s
		ORDER BY embedding <-> $1
		LIMIT $2
	`, s.tableName)

	// Execute the query.
	rows, err := s.pool.Query(ctx, query, vec, maxResults)
	if err != nil {
		return nil, errors.Wrap(err, "executing search query")
	}
	defer rows.Close()

	// Collect the results.
	var results []cspann.KeyBytes
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Wrap(err, "scanning search result")
		}
		results = append(results, id)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterating search results")
	}

	return results, nil
}

// GetMetrics implements the VectorProvider interface.
func (s *SQLProvider) GetMetrics() ([]IndexMetric, error) {
	// Start with our existing metrics
	metrics := []IndexMetric{
		// retryCount is the number of times that InsertVectors encounters
		// conflicts and needs to retry.
		{Name: "insert retries", Value: float64(s.retryCount.Load())},
	}

	// Fetch Prometheus metrics.
	promMetrics, err := s.fetchPrometheusMetrics()
	if err != nil {
		return nil, err
	}

	return append(promMetrics, metrics...), nil
}

// FormatStats implements the VectorProvider interface.
func (s *SQLProvider) FormatStats() string {
	return ""
}

// ensureTableExists creates the table if it doesn't yet exist.
func (s *SQLProvider) ensureTableExists(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BYTES PRIMARY KEY,
			embedding VECTOR(%d),
			VECTOR INDEX (embedding)
		)`, s.tableName, s.dims))

	return errors.Wrap(err, "creating table")
}

// sanitizeIdentifier makes a string safe to use as a SQL identifier
func sanitizeIdentifier(s string) string {
	// Replace non-alphanumeric characters with underscores.
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, s)
}

// Fetch metrics from the Prometheus endpoint.
func (s *SQLProvider) fetchPrometheusMetrics() ([]IndexMetric, error) {
	var metricsToTrack = map[string]float64{
		"sql_vecindex_successful_splits": -1,
		"sql_vecindex_fixups_added":      -1,
		"sql_vecindex_fixups_processed":  -1,
	}

	// Parse connection string to extract host.
	config, err := pgxpool.ParseConfig(*flagDBConnStr)
	if err != nil {
		return nil, errors.Wrap(err, "parsing connection string")
	}

	// Extract host and convert SQL port to CockroachDB HTTP port.
	host := config.ConnConfig.Host
	port := "8080"

	url := fmt.Sprintf("http://%s:%s/_status/vars", host, port)

	// Fetch metrics
	resp, err := httputil.Get(context.Background(), url)
	if err != nil {
		return nil, errors.Wrap(err, "fetching metrics")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("unexpected status code: %d", resp.StatusCode)
	}

	// Parse metrics.
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()

		// Skip comments and empty lines.
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Parse metric line (format: metric_name value).
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		metricName := parts[0]

		// Trim any labels.
		labelOffset := strings.Index(metricName, "{")
		if labelOffset > 0 {
			metricName = line[:labelOffset]
		}

		// Check if this is a metric we want to track and parse its value.
		if _, ok := metricsToTrack[metricName]; ok {
			var value float64
			if _, err := fmt.Sscanf(parts[1], "%f", &value); err == nil {
				metricsToTrack[metricName] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scanning metrics")
	}

	// Construct vecbench metrics from the raw CRDB metrics.
	var metrics []IndexMetric
	if splits, ok := metricsToTrack["sql_vecindex_successful_splits"]; ok {
		metrics = append(metrics, IndexMetric{Name: "successful splits", Value: splits})
	}
	if added, ok := metricsToTrack["sql_vecindex_fixups_added"]; ok {
		if processed, ok := metricsToTrack["sql_vecindex_fixups_processed"]; ok {
			fixupQueueSize := max(added-processed, 0)
			metrics = append(metrics, IndexMetric{Name: "fixup queue size", Value: fixupQueueSize})
		}
	}

	return metrics, nil
}
