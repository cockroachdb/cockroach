// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// sqlDialect encapsulates database-specific behavior so that SQLProvider can
// work with different SQL backends (CockroachDB, PostgreSQL+pgvector, etc.)
// without conditional branching. Adding a new database backend requires only
// implementing this interface.
type sqlDialect interface {
	// setup runs one-time database initialization such as enabling extensions
	// or cluster settings.
	setup(ctx context.Context, pool *pgxpool.Pool) error

	// createIndexQuery returns the SQL statement to create a vector index.
	createIndexQuery(
		indexName, tableName string, distMetric vecpb.DistanceMetric,
	) string

	// checkIndexCreationStatus returns the progress (0.0–1.0) of an
	// asynchronous index creation job. Returns -1 if the database does not
	// support progress tracking (e.g. synchronous index creation).
	checkIndexCreationStatus(ctx context.Context, pool *pgxpool.Pool) (float64, error)

	// formatVectorParam converts a vector into the query parameter format
	// expected by the database driver.
	formatVectorParam(vec vector.T) any

	// setSearchEffort configures the per-session search effort on the given
	// connection.
	setSearchEffort(ctx context.Context, conn *pgxpool.Conn, beamSize int) error

	// additionalMetrics returns database-specific metrics beyond the common
	// ones tracked by SQLProvider (e.g. retry count).
	additionalMetrics() ([]IndexMetric, error)
}

// crdbDialect implements sqlDialect for CockroachDB.
type crdbDialect struct{}

func (d *crdbDialect) setup(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "SET CLUSTER SETTING feature.vector_index.enabled = true")
	return errors.Wrap(err, "enabling vector indexes")
}

func (d *crdbDialect) createIndexQuery(
	indexName, tableName string, distMetric vecpb.DistanceMetric,
) string {
	var opClass string
	switch distMetric {
	case vecpb.CosineDistance:
		opClass = " vector_cosine_ops"
	case vecpb.InnerProductDistance:
		opClass = " vector_ip_ops"
	case vecpb.L2SquaredDistance:
		// L2 is the default operator class in CockroachDB, no suffix needed.
	default:
		panic(fmt.Sprintf("unsupported distance metric: %v", distMetric))
	}
	return fmt.Sprintf(
		"CREATE VECTOR INDEX %s ON %s (embedding%s)",
		indexName, tableName, opClass,
	)
}

func (d *crdbDialect) checkIndexCreationStatus(
	ctx context.Context, pool *pgxpool.Pool,
) (float64, error) {
	var status string
	var fractionCompleted float64

	rows, err := pool.Query(ctx, `
		SELECT status, fraction_completed
		FROM [SHOW JOBS]
		WHERE description LIKE '%%vecbench%%'
		AND job_type = 'NEW SCHEMA CHANGE'
		ORDER BY created DESC
		LIMIT 1`)
	if err != nil {
		return 0, errors.Wrap(err, "querying job progress")
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, errors.Wrap(err, "querying job progress")
		}
		return 0.0, nil
	}

	if err := rows.Scan(&status, &fractionCompleted); err != nil {
		return 0, errors.Wrap(err, "scanning job progress")
	}

	if status == "succeeded" {
		return 1.0, nil
	}
	if status == "failed" || status == "canceled" {
		return fractionCompleted, errors.Newf(
			"index creation job failed with status: %s", status)
	}

	return fractionCompleted, nil
}

func (d *crdbDialect) formatVectorParam(vec vector.T) any {
	return vec
}

func (d *crdbDialect) setSearchEffort(ctx context.Context, conn *pgxpool.Conn, beamSize int) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("SET vector_search_beam_size = %d", beamSize))
	return errors.Wrap(err, "setting vector_search_beam_size")
}

// additionalMetrics fetches vector index metrics from the CockroachDB
// Prometheus endpoint.
func (d *crdbDialect) additionalMetrics() ([]IndexMetric, error) {
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

	resp, err := httputil.Get(context.Background(), url)
	if err != nil {
		return nil, errors.Wrap(err, "fetching metrics")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("unexpected status code: %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

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

	metrics := []IndexMetric{
		{Name: "successful splits", Value: metricsToTrack["sql_vecindex_successful_splits"]},
		{Name: "pending splits/merges", Value: metricsToTrack["sql_vecindex_pending_splits_merges"]},
	}

	return metrics, nil
}

// pgvectorDialect implements sqlDialect for PostgreSQL with the pgvector
// extension.
type pgvectorDialect struct{}

func (d *pgvectorDialect) setup(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	return errors.Wrap(err, "creating pgvector extension")
}

func (d *pgvectorDialect) createIndexQuery(
	indexName, tableName string, distMetric vecpb.DistanceMetric,
) string {
	var opClass string
	switch distMetric {
	case vecpb.L2SquaredDistance:
		opClass = "vector_l2_ops"
	case vecpb.CosineDistance:
		opClass = "vector_cosine_ops"
	case vecpb.InnerProductDistance:
		opClass = "vector_ip_ops"
	default:
		panic(fmt.Sprintf("unsupported distance metric: %v", distMetric))
	}
	return fmt.Sprintf(
		"CREATE INDEX %s ON %s USING hnsw (embedding %s)",
		indexName, tableName, opClass,
	)
}

func (d *pgvectorDialect) checkIndexCreationStatus(
	ctx context.Context, pool *pgxpool.Pool,
) (float64, error) {
	// pgvector index creation is synchronous — CREATE INDEX blocks until
	// complete. Return -1 to indicate progress tracking is not available.
	return -1, nil
}

func (d *pgvectorDialect) formatVectorParam(vec vector.T) any {
	// pgvector needs the vector as a text string (e.g. "[1,2,3]") since pgx
	// would otherwise send []float32 as a float4[] array, which PostgreSQL
	// won't auto-cast to the vector type.
	return vec.String()
}

func (d *pgvectorDialect) setSearchEffort(
	ctx context.Context, conn *pgxpool.Conn, beamSize int,
) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("SET hnsw.ef_search = %d", beamSize))
	return errors.Wrap(err, "setting hnsw.ef_search")
}

func (d *pgvectorDialect) additionalMetrics() ([]IndexMetric, error) {
	// PostgreSQL doesn't expose CRDB-specific Prometheus metrics.
	return nil, nil
}
