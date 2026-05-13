// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
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
// CockroachDB or PostgreSQL+pgvector instance. Database-specific behavior is
// delegated to the sqlDialect interface.
type SQLProvider struct {
	datasetName  string
	dims         int
	distMetric   vecpb.DistanceMetric
	options      cspann.IndexOptions
	pool         *pgxpool.Pool
	tableName    string
	indexName    string
	retryCount   atomic.Uint64
	dialect      sqlDialect
	followerRead bool
}

// NewSQLProvider creates a new SQLProvider that connects to a CockroachDB or
// PostgreSQL instance.
func NewSQLProvider(
	ctx context.Context,
	datasetName string,
	dims int,
	distMetric vecpb.DistanceMetric,
	options cspann.IndexOptions,
	followerRead bool,
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
	indexName := fmt.Sprintf("vecbench_%s_embedding_idx", sanitizeIdentifier(datasetName))

	// Select the dialect based on the --pgvector flag.
	var dialect sqlDialect
	if *flagPgvector {
		dialect = &pgvectorDialect{}
	} else {
		dialect = &crdbDialect{}
	}

	return &SQLProvider{
		datasetName:  datasetName,
		distMetric:   distMetric,
		dims:         dims,
		options:      options,
		pool:         pool,
		tableName:    tableName,
		indexName:    indexName,
		dialect:      dialect,
		followerRead: followerRead,
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

	if err := s.dialect.setup(ctx, s.pool); err != nil {
		return errors.Wrap(err, "database setup")
	}

	// Create table without vector index. Index creation is handled separately.
	// Use BYTEA for the id column, which is compatible with both CockroachDB
	// (where it is an alias for BYTES) and PostgreSQL.
	_, err = s.pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id BYTEA PRIMARY KEY,
		embedding VECTOR(%d))`, s.tableName, s.dims))
	return errors.Wrap(err, "creating table")
}

// CreateIndex implements the VectorProvider interface.
func (s *SQLProvider) CreateIndex(ctx context.Context) error {
	query := s.dialect.createIndexQuery(s.indexName, s.tableName, s.distMetric)
	_, err := s.pool.Exec(ctx, query)
	return errors.Wrap(err, "creating index")
}

// CheckIndexCreationStatus implements the VectorProvider interface.
func (s *SQLProvider) CheckIndexCreationStatus(ctx context.Context) (float64, error) {
	return s.dialect.checkIndexCreationStatus(ctx, s.pool)
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
		args[j+1] = s.dialect.formatVectorParam(vectors.At(i))
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

	if err := s.dialect.setSearchEffort(ctx, conn, beamSize); err != nil {
		return nil, errors.Wrap(err, "configuring search effort")
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
	var aost string
	if s.followerRead {
		aost = " AS OF SYSTEM TIME follower_read_timestamp()"
	}
	query := fmt.Sprintf(`
		SELECT id
		FROM %s%s
		ORDER BY embedding %s $1
		LIMIT %d
	`, s.tableName, aost, op, maxResults)

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

	rows, err := sqlState.conn.Query(
		ctx, sqlState.query, s.dialect.formatVectorParam(vec))
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
	// Start with the provider's own metrics.
	metrics := []IndexMetric{
		// retryCount is the number of times that InsertVectors encounters
		// conflicts and needs to retry.
		{Name: "insert retries", Value: float64(s.retryCount.Load())},
	}

	additionalMetrics, err := s.dialect.additionalMetrics()
	if err != nil {
		return nil, err
	}

	return append(additionalMetrics, metrics...), nil
}

// FormatStats implements the VectorProvider interface.
func (s *SQLProvider) FormatStats() string {
	return ""
}

// sanitizeIdentifier makes a string safe to use as a SQL identifier.
func sanitizeIdentifier(s string) string {
	// Replace non-alphanumeric characters with underscores.
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, s)
}
