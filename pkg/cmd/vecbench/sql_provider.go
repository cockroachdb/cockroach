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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLSearchState holds an open connection and a prepared statement to run on
// that connection in repeated calls to Search.
type SQLSearchState struct {
	conn  *pgxpool.Conn
	query string
}

// Close releases the connection back to the pool.
func (s *SQLSearchState) Close() {
	if s.conn != nil {
		s.conn.Release()
		s.conn = nil
	}
}

// SQLProvider implements VectorProvider using a SQL database connection to a
// CockroachDB instance.
type SQLProvider struct {
	datasetName string
	dims        int
	distMetric  vecpb.DistanceMetric
	options     cspann.IndexOptions
	pool        *pgxpool.Pool
	tableName   string
	retryCount  atomic.Uint64
}

// NewSQLProvider creates a new SQLProvider that connects to a CockroachDB
// instance.
func NewSQLProvider(
	ctx context.Context,
	datasetName string,
	dims int,
	distMetric vecpb.DistanceMetric,
	options cspann.IndexOptions,
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
		distMetric:  distMetric,
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

// New implements the VectorProvider interface.
func (s *SQLProvider) New(ctx context.Context) error {
	// Drop the table if it exists.
	_, err := s.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", s.tableName))
	if err != nil {
		return errors.Wrap(err, "dropping table")
	}

	// Create table without vector index.
	_, err = s.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id BYTES PRIMARY KEY,
			embedding VECTOR(%d)
		)`, s.tableName, s.dims))
	return errors.Wrap(err, "creating table")
}

// InsertVectors implements the VectorProvider interface.
func (s *SQLProvider) InsertVectors(
	ctx context.Context, keys []cspann.KeyBytes, vectors vector.Set,
) error {
	// Build insert query.
	args := make([]any, vectors.Count*2)
	var queryBuilder strings.Builder
	queryBuilder.Grow(100 + vectors.Count*12)
	queryBuilder.WriteString("INSERT INTO ")
	queryBuilder.WriteString(s.tableName)
	queryBuilder.WriteString(" (id, embedding) VALUES")
	for i := range vectors.Count {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		j := i * 2
		fmt.Fprintf(&queryBuilder, " ($%d, $%d)", j+1, j+2)
		args[j] = keys[i]
		args[j+1] = vectors.At(i)
	}
	query := queryBuilder.String()

	// Retry loop.
	for {
		_, err := s.pool.Exec(ctx, query, args...)

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

// Finalize implements the VectorProvider interface.
func (s *SQLProvider) Finalize(ctx context.Context) error {
	// Enable vector indexes if not already enabled.
	_, err := s.pool.Exec(ctx, "SET CLUSTER SETTING feature.vector_index.enabled = true")
	if err != nil {
		return errors.Wrap(err, "enabling vector indexes")
	}

	var opClass string
	switch s.distMetric {
	case vecpb.CosineDistance:
		opClass = " vector_cosine_ops"
	case vecpb.InnerProductDistance:
		opClass = " vector_ip_ops"
	}

	indexName := fmt.Sprintf("vecbench_%s_embedding_idx", sanitizeIdentifier(s.datasetName))

	// Use a WaitGroup to wait for progress tracking to complete.
	var wg sync.WaitGroup
	var progressErr error

	// Track progress of the index creation job.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.trackIndexCreationProgress(ctx, indexName); err != nil {
			progressErr = err
		}
	}()

	_, err = s.pool.Exec(ctx, fmt.Sprintf(`
		CREATE VECTOR INDEX %s ON %s (embedding%s)`, indexName, s.tableName, opClass))
	if err != nil {
		return errors.Wrap(err, "creating vector index")
	}

	// Wait for progress tracking to complete.
	wg.Wait()
	if progressErr != nil {
		return errors.Wrap(progressErr, "tracking index creation progress")
	}

	return nil
}

// trackIndexCreationProgress polls SHOW JOBS to track vector index creation progress.
func (s *SQLProvider) trackIndexCreationProgress(ctx context.Context, indexName string) error {
	fmt.Printf(White+"\nCreating vector index %s...\n"+Reset, indexName)

	var startAt crtime.Mono
	for {
		time.Sleep(time.Second)

		var status string
		var fractionCompleted float64
		var runningStatus string

		// Query for jobs related to our index creation.
		rows, err := s.pool.Query(ctx, `
			SELECT status, fraction_completed, running_status
			FROM [SHOW JOBS]
			WHERE description LIKE '%%vecbench%%'
			AND job_type = 'NEW SCHEMA CHANGE'
			ORDER BY created DESC
			LIMIT 1`)
		if err != nil {
			return errors.Wrap(err, "querying job progress")
		}

		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return errors.Wrap(err, "querying job progress")
			} else if startAt != 0 {
				return errors.Newf("index creation job disappeared unexpectedly")
			} else if startAt.Elapsed().Seconds() > 5 {
				return errors.Newf("index creation job failed to start")
			}
			continue
		}

		if startAt == 0 {
			startAt = crtime.NowMono()
		}
		if err := rows.Scan(&status, &fractionCompleted, &runningStatus); err != nil {
			return errors.Wrap(err, "scanning job progress")
		}
		rows.Close()

		// Display progress.
		progressPercent := fractionCompleted * 100
		fmt.Printf(ClearLine+White+"\rIndex creation progress: %.1f%% (%s) in %v"+Reset,
			progressPercent, status, roundDuration(startAt.Elapsed()))

		if status == "succeeded" {
			break
		} else if status != "running" {
			fmt.Printf(Red+"\nVector index creation failed with status: %s: %s\n"+Reset, status, runningStatus)
			return errors.Newf("index creation job failed with status: %s", status)
		}
	}

	fmt.Printf(White + "\nVector index creation completed successfully.\n" + Reset)
	return nil
}

// SetupSearch implements the VectorProvider interface.
func (s *SQLProvider) SetupSearch(
	ctx context.Context, maxResults int, beamSize int,
) (SearchState, error) {
	// Acquire a connection from the pool.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "acquiring connection from pool")
	}
	defer func() {
		if conn != nil {
			conn.Release()
		}
	}()

	// Set the vector_search_beam_size session variable.
	_, err = conn.Exec(ctx, fmt.Sprintf("SET vector_search_beam_size = %d", beamSize))
	if err != nil {
		return nil, errors.Wrap(err, "setting vector_search_beam_size")
	}

	var op string
	switch s.distMetric {
	case vecpb.L2SquaredDistance:
		op = "<->"
	case vecpb.CosineDistance:
		op = "<=>"
	case vecpb.InnerProductDistance:
		op = "<#>"
	}

	// Construct the query for vector search.
	query := fmt.Sprintf(`
		SELECT id
		FROM %s
		ORDER BY embedding %s $1
		LIMIT %d
	`, s.tableName, op, maxResults)

	state := &SQLSearchState{
		conn:  conn,
		query: query,
	}
	conn = nil
	return state, nil
}

// Search implements the VectorProvider interface.
func (s *SQLProvider) Search(
	ctx context.Context, state SearchState, vec vector.T, stats *cspann.SearchStats,
) ([]cspann.KeyBytes, error) {
	sqlState, ok := state.(*SQLSearchState)
	if !ok {
		return nil, errors.New("invalid search state type")
	}

	// Execute the prepared statement.
	rows, err := sqlState.conn.Query(ctx, sqlState.query, vec)
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
		"sql_vecindex_successful_splits":     -1,
		"sql_vecindex_pending_splits_merges": -1,
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
	if pending, ok := metricsToTrack["sql_vecindex_pending_splits_merges"]; ok {
		metrics = append(metrics, IndexMetric{Name: "pending splits/merges", Value: pending})
	}

	return metrics, nil
}
