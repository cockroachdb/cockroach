// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"archive/zip"
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type profileOptions struct {
	dbName               string
	probabilityToInclude float64
	minNumExpectedStmts  int
	minimumLatency       time.Duration
	multipleFromP99      int
}

type ProfileOptionFunc func(*profileOptions)

// profDbName is the database name to profile against.
func ProfDbName(dbName string) ProfileOptionFunc {
	return func(o *profileOptions) {
		o.dbName = dbName
	}
}

// profProbabilityToInclude is the probability that a statement will have
// tracing started on it.
func ProfProbabilityToInclude(probabilityToInclude float64) ProfileOptionFunc {
	return func(o *profileOptions) {
		o.probabilityToInclude = probabilityToInclude
	}
}

// profMinNumExpectedStmtsis the minimum number of times the statement must be
// executed to be included. By using a count here it removes the need to
// explicitly list out all the statements we need to capture.
func ProfMinNumExpectedStmts(minNumExpectedStmts int) ProfileOptionFunc {
	return func(o *profileOptions) {
		o.minNumExpectedStmts = minNumExpectedStmts
	}
}

// profMinimumLatency is the minimum P99 latency in seconds. Anything lower than
// this is rounded up to this value.
func ProfMinimumLatency(minimumLatency time.Duration) ProfileOptionFunc {
	return func(o *profileOptions) {
		o.minimumLatency = minimumLatency
	}
}

// profMultipleFromP99is the multiple of the P99 latency that the statement must
// exceed in order to be collected. NB: This doesn't really work well. The data
// in statement_statistics is not very accurate so we often use the minimum
// latency of 10ms.
func ProfMultipleFromP99(multipleFromP99 int) ProfileOptionFunc {
	return func(o *profileOptions) {
		o.multipleFromP99 = multipleFromP99
	}
}

// ProfileTopStatements enables profile collection on the top statements from
// the cluster that exceed 50ms latency and are more than 10x the P99 up to this
// point. Top statements are defined as ones that have executed frequently
// enough to matter.
func ProfileTopStatements(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger, opt ...ProfileOptionFunc,
) error {
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()

	// Enable continuous statement diagnostics rather than just the first one.
	sql := "SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled=true"
	if _, err := db.ExecContext(ctx, sql); err != nil {
		return err
	}

	// The default values for the profile options trace statements over 50ms.
	// The options can be changed by passing additional profileOptionFuncs.
	opts := profileOptions{
		dbName:               "defaultdb",
		probabilityToInclude: 0.001,
		minNumExpectedStmts:  1000,
		minimumLatency:       50 * time.Millisecond,
		multipleFromP99:      10,
	}
	for _, f := range opt {
		f(&opts)
	}

	sql = fmt.Sprintf(`
SELECT
    crdb_internal.request_statement_bundle(
		statement,
		%f,
		(CASE WHEN latency > max_latency THEN latency ELSE max_latency END)::INTERVAL,
		'12h'::INTERVAL
	)
FROM (
    SELECT max(p99)::FLOAT*%d AS latency, %f AS max_latency, statement FROM (
		SELECT
			metadata->>'db' AS db,
			metadata->>'query' AS statement,
			CAST(statistics->'execution_statistics'->>'cnt' AS int) AS cnt,
			statistics->'statistics'->'latencyInfo'->'p99' AS p99
			FROM crdb_internal.statement_statistics
		)
	WHERE db = '%s' AND cnt > %d AND p99::FLOAT > 0
	GROUP BY statement
)`,
		opts.probabilityToInclude,
		opts.multipleFromP99,
		opts.minimumLatency.Seconds(),
		opts.dbName,
		opts.minNumExpectedStmts,
	)
	if _, err := db.ExecContext(ctx, sql); err != nil {
		return err
	}
	return nil
}

// DownloadProfiles downloads all profiles from the cluster and saves them to
// the given artifacts directory to the stmtbundle sub-directory.
func DownloadProfiles(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger, outputDir string,
) error {
	stmtDir := filepath.Join(outputDir, "stmtbundle")
	if err := os.MkdirAll(stmtDir, os.ModePerm); err != nil {
		return err
	}
	query := "SELECT id, collected_at FROM system.statement_diagnostics"
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()
	idRow, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	adminUIAddrs, err := cluster.ExternalAdminUIAddr(ctx, logger, cluster.Node(1))
	if err != nil {
		return err
	}

	client := DefaultHTTPClient(cluster, logger)
	urlPrefix := `https://` + adminUIAddrs[0] + `/_admin/v1/stmtbundle/`

	var diagID string
	var collectedAt time.Time
	for idRow.Next() {
		if err := idRow.Scan(&diagID, &collectedAt); err != nil {
			return err
		}
		url := urlPrefix + diagID
		if err := downloadProfile(ctx, logger, client, url, collectedAt, stmtDir); err != nil {
			return err
		}
	}
	return nil
}

func downloadProfile(
	ctx context.Context,
	logger *logger.Logger,
	client *RoachtestHTTPClient,
	url string,
	collectedAt time.Time,
	targetDir string,
) error {
	resp, err := client.Get(ctx, url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Copy the contents of the URL to a BytesBuffer to determine the
	// filename before saving it below.
	var buf bytes.Buffer
	_, err = io.Copy(&buf, resp.Body)
	if err != nil {
		return err
	}
	filename, err := getFilename(collectedAt, buf)
	if err != nil {
		return err
	}
	// write the buf to the filename
	file, err := os.Create(filepath.Join(targetDir, filename))
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, &buf); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	logger.Printf("downloaded profile %s", filename)
	return nil
}

// getFilename creates a file name for the profile based on the traced operation
// and duration. An example filename is
// 2024-10-24T18_23_57Z-UPSERT-101.490ms.zip.
func getFilename(collectedAt time.Time, buf bytes.Buffer) (string, error) {
	// Download the zip to a BytesBuffer.
	unzip, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		return "", err
	}
	// NB: The format of the trace.txt file is not guaranteed to be stable. If
	// this proves problematic we could parse the trace.json instead. Parsing
	// the trace.txt is easier due to the nested structure of the trace.json.
	r, err := unzip.Open("trace.txt")
	if err != nil {
		return "", err
	}
	bytes, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	if err = r.Close(); err != nil {
		return "", err
	}
	lines := strings.Split(string(bytes), "\n")
	// The first line is the SQL statement. An example is `UPSERT INTO kv (k, v)
	// VALUES ($1, $2)`. We only grab the operation to help differentiate
	// traces. An alternative if this isn't differentiated enough is to use the
	// entire fingerprint text, however that creates longs and complex
	// filenames.
	operation := strings.Split(strings.TrimSpace(lines[0]), " ")[0]
	// Use the second to last line because the last line is empty.
	duration := strings.Split(strings.TrimSpace(lines[len(lines)-2]), " ")[0]
	return fmt.Sprintf("%s-%s-%s.zip", collectedAt.Format("2006-01-02T15_04_05Z07:00"), operation, duration), nil
}

// MeasureQPS will measure the approx QPS at the time this command is run. The
// duration is the interval to measure over. Setting too short of an interval
// can mean inaccuracy in results. Setting too long of an interval may mean the
// impact is blurred out.
func MeasureQPS(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	duration time.Duration,
	nodes option.NodeListOption,
) float64 {
	totalOpsCompleted := func() int {
		// NB: We mgight not be able to hold the connection open during the full duration.
		var dbs []*gosql.DB
		for _, nodeId := range nodes {
			db := c.Conn(ctx, t.L(), nodeId)
			defer db.Close() //nolint:deferloop
			dbs = append(dbs, db)
		}
		// Count the inserts before sleeping.
		var total atomic.Int64
		group := ctxgroup.WithContext(ctx)
		for _, db := range dbs {
			group.Go(func() error {
				var v float64
				if err := db.QueryRowContext(
					ctx, `SELECT sum(value) FROM crdb_internal.node_metrics WHERE name in ('sql.select.count', 'sql.insert.count')`,
				).Scan(&v); err != nil {
					return err
				}
				total.Add(int64(v))
				return nil
			})
		}

		require.NoError(t, group.Wait())
		return int(total.Load())
	}

	// Measure the current time and the QPS now.
	startTime := timeutil.Now()
	beforeOps := totalOpsCompleted()
	// Wait for the duration minus the first query time.
	select {
	case <-ctx.Done():
		return 0
	case <-time.After(duration - timeutil.Since(startTime)):
		afterOps := totalOpsCompleted()
		return float64(afterOps-beforeOps) / duration.Seconds()
	}
}

// getProfileWithTimeout is a helper function that receives an HTTP client and a
// profile request URL, and returns the profile data. It keeps trying until
// it succeeds or the timeout is reached.
func getProfileWithTimeout(
	ctx context.Context,
	logger *logger.Logger,
	client *RoachtestHTTPClient,
	url string,
	timeout time.Duration,
) (*profile.Profile, error) {
	logger.Printf("getting profile using URL: %s", url)

	// Set up a timer to limit the time spent trying to get the profile.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var latestError error
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrapf(errors.CombineErrors(latestError, ctx.Err()),
				"failed to get profile from %s after %s", url, timeout)
		case <-timer.C:
			return nil, errors.Wrapf(errors.CombineErrors(latestError, errors.New("timed out")),
				"failed to get profile from %s after %s", url, timeout)
		default:
		}

		// Request the profile.
		resp, err := client.Get(ctx, url)
		if err != nil {
			latestError = err
			resp.Body.Close()
			continue
		}

		var buf bytes.Buffer
		_, err = io.Copy(&buf, resp.Body)
		if err != nil {
			latestError = err
			resp.Body.Close()
			continue
		}

		// Parse the profile data. This ensures that the data is valid and not
		// corrupt.
		prof, err := profile.ParseData(buf.Bytes())
		if err != nil {
			latestError = err
			resp.Body.Close()
			continue
		}

		resp.Body.Close()
		return prof, nil
	}
}

// getProfileSingleNode returns the specified profile for a single node. If
// taking the profile fails, we will keep retrying for 15 seconds.
// Supported profile types are: {"cpu", "allocs", "mutex", "heap"}.
func getProfileSingleNode(
	ctx context.Context,
	cluster cluster.Cluster,
	logger *logger.Logger,
	profileType string,
	nodeID int,
	duration time.Duration,
) (*profile.Profile, error) {
	var actualProfileType string
	switch profileType {
	case "cpu":
		// It's unfortunate that the CPU profile endpoint is called "profile".
		actualProfileType = "profile"
	case "allocs":
		actualProfileType = "allocs"
	case "mutex":
		actualProfileType = "mutex"
	case "heap":
		actualProfileType = "heap"
	default:
		return nil, errors.Newf("invalid profile type %s", profileType)
	}
	adminUIAddrs, err := cluster.ExternalAdminUIAddr(ctx, logger, cluster.Node(nodeID))
	if err != nil {
		return nil, err
	}
	// Use a long enough HTTP timeout that is larger than the profile duration.
	client := DefaultHTTPClient(cluster, logger, HTTPTimeout(2*duration))
	url := fmt.Sprintf("https://%s/debug/pprof/%s?seconds=%d", adminUIAddrs[0],
		actualProfileType, int(duration.Seconds()))
	return getProfileWithTimeout(ctx, logger, client, url, 15*time.Second /* timeout */)
}

// GetProfile collect profiles for all the nodes received in the function
// parameters. It returns a slice of profiles or an error if any of the
// profiles failed to collect. Each entry in the slices is in the same order as
// the nodes received.
// Supported profile types are: {"cpu", "allocs", "mutex", "heap"}.
func GetProfile(
	ctx context.Context,
	cluster cluster.Cluster,
	logger *logger.Logger,
	profileType string,
	duration time.Duration,
	nodes option.NodeListOption,
) ([]*profile.Profile, error) {
	profiles := make([]*profile.Profile, len(nodes))

	// Create an error group to manage concurrent profile collection.
	g, ctx := errgroup.WithContext(ctx)

	for i, nodeId := range nodes {
		g.Go(func() error {
			var err error
			profiles[i], err = getProfileSingleNode(ctx, cluster, logger, profileType,
				nodeId, duration)

			if err != nil {
				logger.Printf("error getting profile for node %d: %s", nodeId, err)
				return errors.Wrapf(err, "getting profile for n%d", nodeId)
			}
			return nil
		})
	}

	// Wait for all profiles to complete or first error
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return profiles, nil
}

// ExportProfile exports a profile to specified destination directory.
func ExportProfile(profile *profile.Profile, destinationDir string, filename string) error {
	buf := bytes.Buffer{}
	if err := profile.Write(&buf); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(destinationDir, filename), buf.Bytes(), 0644)
}
