// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGetStatementFingerprints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	ts := testCluster.Server(0)
	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)

	// Insert test data
	setupFingerprintTestData(t, conn)

	t.Run("non GET method 405 error", func(t *testing.T) {
		req, err := http.NewRequest("POST", ts.AdminURL().WithPath("/api/v2/fingerprints/").String(), nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.NotNil(t, resp)
		require.Equal(t, 405, resp.StatusCode)
	})

	t.Run("basic retrieval", func(t *testing.T) {
		uri := "/api/v2/fingerprints/"
		mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
		require.NotEmpty(t, mdResp.Results)
		require.Greater(t, mdResp.PaginationInfo.TotalResults, int64(0))
	})

	t.Run("pagination", func(t *testing.T) {
		var pageTests = []struct {
			name             string
			queryString      string
			expectedPageNum  int
			expectedPageSize int
		}{
			{"no page size or page num", "?", defaultPageNum, defaultPageSize},
			{"set page size", "?pageSize=2", defaultPageNum, 2},
			{"set page size and page num", "?pageSize=2&pageNum=2", 2, 2},
			{"invalid page size and num", "?pageSize=0&pageNum=0", defaultPageNum, defaultPageSize},
		}
		for _, tt := range pageTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/fingerprints/%s", tt.queryString)
				mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
				require.NotEmpty(t, mdResp.Results)
				require.LessOrEqual(t, len(mdResp.Results), tt.expectedPageSize)
				require.Equal(t, tt.expectedPageSize, mdResp.PaginationInfo.PageSize)
				require.Equal(t, tt.expectedPageNum, mdResp.PaginationInfo.PageNum)
			})
		}

		t.Run("large page num", func(t *testing.T) {
			uri := "/api/v2/fingerprints/?pageSize=1&pageNum=100"
			mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
			require.Empty(t, mdResp.Results)
		})

	})

	t.Run("fuzzy search", func(t *testing.T) {
		var searchTests = []struct {
			name          string
			searchQuery   string
			expectedCount int
			shouldContain string
		}{
			{"fuzzy search", "SELECT mytable", 3, "SELECT"},
			{"non-fuzzy search", "FROM mytable WHERE", 2, "mytable"},
		}

		for _, tt := range searchTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/fingerprints/?search=%s", tt.searchQuery)
				mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
				fmt.Printf("results: %+v\n", mdResp.Results)
				require.Equal(t, int64(tt.expectedCount), mdResp.PaginationInfo.TotalResults)
				if len(mdResp.Results) > 0 {
					// At least one result should contain the search term
					found := false
					for _, fp := range mdResp.Results {
						if contains(fp.Query, tt.shouldContain) {
							found = true
							break
						}
					}
					require.True(t, found, "expected to find '%s' in at least one fingerprint", tt.shouldContain)
				}
			})
		}

		t.Run("no results", func(t *testing.T) {
			uri := "/api/v2/fingerprints/?search=NONEXISTENT_QUERY_PATTERN"
			mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
			require.Empty(t, mdResp.Results)
			require.Equal(t, int64(0), mdResp.PaginationInfo.TotalResults)
		})

	})

	t.Run("400 bad request", func(t *testing.T) {
		var badRequestTests = []struct {
			name        string
			queryString string
		}{
			{"invalid pageNum", "?pageNum=abc"},
			{"invalid pageSize", "?pageSize=xyz"},
		}
		for _, tt := range badRequestTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/fingerprints/%s", tt.queryString)
				req, err := http.NewRequest("GET", ts.AdminURL().WithPath(uri).String(), nil)
				require.NoError(t, err)
				resp, err := client.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			})
		}
	})

	t.Run("response fields", func(t *testing.T) {
		uri := "/api/v2/fingerprints/?pageSize=1"
		mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
		require.Len(t, mdResp.Results, 1)

		fp := mdResp.Results[0]
		require.NotEmpty(t, fp.Fingerprint)
		require.NotEmpty(t, fp.Query)
		//require.NotEmpty(t, fp.Summary)
		//require.NotEmpty(t, fp.Database)
		require.False(t, fp.CreatedAt.IsZero())
	})

	t.Run("ordering without search", func(t *testing.T) {
		uri := "/api/v2/fingerprints/?pageSize=100"
		mdResp := makeApiRequest[PaginatedResponse[[]StatementFingerprint]](t, client, ts.AdminURL().WithPath(uri).String(), http.MethodGet)
		require.NotEmpty(t, mdResp.Results)

		// Verify results are ordered by created_at descending
		for i := 0; i < len(mdResp.Results)-1; i++ {
			current := mdResp.Results[i].CreatedAt
			next := mdResp.Results[i+1].CreatedAt
			require.True(t, current.After(next) || current.Equal(next),
				"expected results ordered by created_at descending, but %v is not >= %v", current, next)
		}
	})
}

// setupFingerprintTestData inserts test fingerprint data into the system.statement_fingerprints table.
func setupFingerprintTestData(t *testing.T, conn *sqlutils.SQLRunner) {
	t.Helper()

	// Insert test fingerprints with various patterns
	now := time.Now()
	conn.Exec(t, `
INSERT INTO system.statement_fingerprints (row_id, fingerprint, query, summary, implicit_txn, database, created_at)
VALUES
	(nextval('system.statement_fingerprint_id_seq'), b'\x01', 'SELECT _ FROM mytable WHERE id = _', 'select from mytable', true, 'testdb', $1),
	(nextval('system.statement_fingerprint_id_seq'), b'\x02', 'INSERT INTO mytable (name, email) VALUES (_, _)', 'insert into mytable', false, 'testdb', $2),
	(nextval('system.statement_fingerprint_id_seq'), b'\x03', 'UPDATE mytable SET name = _ WHERE id = _', 'update mytable', false, 'testdb', $3),
	(nextval('system.statement_fingerprint_id_seq'), b'\x04', 'SELECT _ FROM mytable_orders WHERE user_id = _', 'select from mytable_orders', true, 'testdb', $4),
	(nextval('system.statement_fingerprint_id_seq'), b'\x05', 'SELECT _ FROM mytable WHERE email = _', 'select from mytable by email', true, 'testdb', $5)
	`, now.Add(-4*time.Minute), now.Add(-3*time.Minute), now.Add(-2*time.Minute), now.Add(-1*time.Minute), now)
}

// contains is a helper function to check if a string contains a substring (case-insensitive).
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		len(s) > len(substr) && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if matchesAt(s, substr, i) {
			return true
		}
	}
	return false
}

func matchesAt(s, substr string, pos int) bool {
	for i := 0; i < len(substr); i++ {
		if toLower(s[pos+i]) != toLower(substr[i]) {
			return false
		}
	}
	return true
}

func toLower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}
