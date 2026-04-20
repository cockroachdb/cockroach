// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testselector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	gosql "database/sql"
	"encoding/pem"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/stretchr/testify/require"
)

func TestCategoriseTests(t *testing.T) {
	_ = os.Unsetenv(sfUsernameEnv)
	_ = os.Unsetenv(sfPrivateKey)
	t.Run("expect getConnect to fail due to missing SNOWFLAKE_USER env", func(t *testing.T) {
		SqlConnectorFunc = nil
		tds, err := CategoriseTests(context.Background(), nil)
		require.Nil(t, tds)
		require.NotNil(t, err)
		require.Equal(t, "environment variable SNOWFLAKE_USER is not set", err.Error())
	})
	_ = os.Setenv(sfUsernameEnv, "dummy_user")
	_ = os.Setenv(sfPrivateKey, createPrivateKey(t))
	t.Run("expect sql connector to fail", func(t *testing.T) {
		SqlConnectorFunc = func(_, _ string) (*gosql.DB, error) {
			return nil, fmt.Errorf("failed to connect to DB")
		}
		tds, err := CategoriseTests(context.Background(), nil)
		require.Nil(t, tds)
		require.NotNil(t, err)
		require.Equal(t, "failed to connect to DB", err.Error())
	})
	var mock sqlmock.Sqlmock
	var db *gosql.DB
	SqlConnectorFunc = func(driverName, dataSourceName string) (*gosql.DB, error) {
		dsn, err := getDSN()
		require.Equal(t, "snowflake", driverName)
		require.Equal(t, dsn, dataSourceName)
		return db, err
	}
	var err error
	t.Run("expect Prepare to fail", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		mock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery)).WillReturnError(fmt.Errorf("failed to prepare"))
		tds, err := CategoriseTests(context.Background(), nil)
		require.Nil(t, tds)
		require.NotNil(t, err)
		require.Equal(t, "failed to prepare", err.Error())
	})
	t.Run("expect query to fail", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		mock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery))
		mock.ExpectQuery(regexp.QuoteMeta(PreparedQuery)).WillReturnError(fmt.Errorf("failed to execute query"))
		tds, err := CategoriseTests(context.Background(), &SelectTestsReq{
			forPastDays: 1,
			firstRunOn:  2,
			lastRunOn:   3,
			cloud:       spec.AWS,
			suite:       "unit test",
		})
		require.Nil(t, tds)
		require.NotNil(t, err)
		require.Equal(t, "failed to execute query", err.Error())
	})
	t.Run("expect the sequence of response list is maintained for success", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		mock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery))
		rows := sqlmock.NewRows(AllRows)
		data := [][]string{
			{"t1", "no", "12345", "no", "2026-01-01"},
			{"t2", "no", "12345", "no", "2026-01-02"},
			{"t3", "no", "12345", "yes", "2026-01-03"},
			{"t4", "no", "12345", "no", ""},
			{"t5", "yes", "12345", "no", "2026-01-05"},
			{"t6", "yes", "12345", "no", "2026-01-06"},
			{"t7", "yes", "12345", "no", "2026-01-07"},
			{"t8", "yes", "12345", "no", "2026-01-08"},
			{"t9", "yes", "12345", "no", "2026-01-09"},
		}
		for _, ds := range data {
			rows.FromCSVString(strings.Join(ds, ","))
		}
		mock.ExpectQuery(regexp.QuoteMeta(PreparedQuery)).WillReturnRows(rows)
		tds, err := CategoriseTests(context.Background(), &SelectTestsReq{
			forPastDays: 1,
			firstRunOn:  2,
			lastRunOn:   3,
			cloud:       spec.AWS,
			suite:       "unit test",
		})
		require.NotNil(t, tds)
		require.Nil(t, err)
		require.Equal(t, len(data), len(tds))
		// the sequence of response list must be maintained.
		for i, d := range data {
			td := tds[i]
			require.Equal(t, d[DataTestNameIndex], td.Name)
			require.Equal(t, d[DataSelectedIndex] != "no", td.Selected)
			require.Equal(t, getDuration(d[DataDurationIndex]), td.AvgDurationInMillis)
			require.Equal(t, d[DataLastPreempted] == "yes", td.LastFailureIsPreempt)
			if d[DataFirstRunIndex] != "" {
				require.NotNil(t, td.FirstRun)
				require.Equal(t, d[DataFirstRunIndex], *td.FirstRun)
			} else {
				require.Nil(t, td.FirstRun)
			}
		}
	})
	t.Run("first run with no snowflake history falls back to master", func(t *testing.T) {
		// Track how many connections are made
		connectionCount := 0
		var currentMock sqlmock.Sqlmock
		var currentDb *gosql.DB

		// Override connector to track connections
		SqlConnectorFunc = func(driverName, dataSourceName string) (*gosql.DB, error) {
			connectionCount++
			if connectionCount == 1 {
				// First connection for release branch query
				currentDb, currentMock, err = sqlmock.New()
				require.Nil(t, err)
				currentMock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery))
				emptyRows := sqlmock.NewRows(AllRows)
				currentMock.ExpectQuery(regexp.QuoteMeta(PreparedQuery)).WillReturnRows(emptyRows)
			} else if connectionCount == 2 {
				// Second connection for master branch fallback
				currentDb, currentMock, err = sqlmock.New()
				require.Nil(t, err)
				currentMock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery))
				masterRows := sqlmock.NewRows(AllRows)
				masterData := [][]string{
					{"acceptance", "yes", "12345", "no", "2026-01-01"},
					{"backup", "no", "12345", "no", "2026-01-02"},
					{"import", "yes", "12345", "no", "2026-01-03"},
					{"kv", "no", "12345", "no", "2026-01-04"},
					{"schemachange", "yes", "12345", "no", "2026-01-05"},
				}
				for _, ds := range masterData {
					masterRows.FromCSVString(strings.Join(ds, ","))
				}
				currentMock.ExpectQuery(regexp.QuoteMeta(PreparedQuery)).WillReturnRows(masterRows)
			}
			return currentDb, nil
		}

		tds, err := CategoriseTests(context.Background(), &SelectTestsReq{
			currentBranch: "release-24.3", // not master
			forPastDays:   1,
			firstRunOn:    0, // disabled for non-master
			lastRunOn:     3,
			cloud:         spec.AWS,
			suite:         "unit test",
		})
		require.Nil(t, err)
		require.NotNil(t, tds)
		require.Equal(t, 2, connectionCount, "Should make two connections: one for release branch, one for master")

		// Should have 5 tests from master branch
		require.Equal(t, 5, len(tds))

		// Verify test names and selection match master data
		require.Equal(t, "acceptance", tds[0].Name)
		require.True(t, tds[0].Selected)
		require.Equal(t, "backup", tds[1].Name)
		require.False(t, tds[1].Selected)
		require.Equal(t, "import", tds[2].Name)
		require.True(t, tds[2].Selected)
		require.Equal(t, "kv", tds[3].Name)
		require.False(t, tds[3].Selected)
		require.Equal(t, "schemachange", tds[4].Name)
		require.True(t, tds[4].Selected)

		// Restore original connector
		SqlConnectorFunc = func(driverName, dataSourceName string) (*gosql.DB, error) {
			dsn, err := getDSN()
			require.Equal(t, "snowflake", driverName)
			require.Equal(t, dsn, dataSourceName)
			return db, err
		}
	})
	t.Run("safety valve triggers when too many never-run tests selected", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		mock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery))
		rows := sqlmock.NewRows(AllRows)
		// Create scenario where 8 out of 10 tests have first_run=null and are selected (80%)
		// This exceeds the 50% threshold, so safety valve should trigger
		data := [][]string{
			{"t1", "yes", "0", "no", ""},                // never run, selected
			{"t2", "yes", "0", "no", ""},                // never run, selected
			{"t3", "yes", "0", "no", ""},                // never run, selected
			{"t4", "yes", "0", "no", ""},                // never run, selected
			{"t5", "yes", "0", "no", ""},                // never run, selected
			{"t6", "yes", "0", "no", ""},                // never run, selected
			{"t7", "yes", "0", "no", ""},                // never run, selected
			{"t8", "yes", "0", "no", ""},                // never run, selected
			{"t9", "no", "12345", "no", "2026-01-01"},   // has run, not selected
			{"t10", "yes", "12345", "no", "2026-01-02"}, // has run, selected
		}
		for _, ds := range data {
			rows.FromCSVString(strings.Join(ds, ","))
		}
		mock.ExpectQuery(regexp.QuoteMeta(PreparedQuery)).WillReturnRows(rows)
		tds, err := CategoriseTests(context.Background(), &SelectTestsReq{
			forPastDays: 1,
			firstRunOn:  2,
			lastRunOn:   3,
			cloud:       spec.AWS,
			suite:       "unit test",
		})
		require.Nil(t, err)
		require.NotNil(t, tds)
		require.Equal(t, len(data), len(tds))

		// Count never-run tests that are still selected after safety valve
		neverRunSelected := 0
		for _, td := range tds {
			if td.Selected && td.FirstRun == nil {
				neverRunSelected++
			}
		}

		// Safety valve should have deselected half of the 8 never-run tests (4 deselected)
		// So we should have 4 never-run selected tests remaining
		// Total selected = 4 (never-run) + 1 (t10 has run) = 5
		require.Equal(t, 4, neverRunSelected, "Safety valve should deselect half of never-run tests")

		totalSelected := 0
		for _, td := range tds {
			if td.Selected {
				totalSelected++
			}
		}
		require.Equal(t, 5, totalSelected, "Total selected should be 5 after safety valve")
	})
	t.Run("expect failure due to invalid private key", func(t *testing.T) {
		SqlConnectorFunc = nil
		_ = os.Setenv(sfPrivateKey, "invalid")
		tds, err := CategoriseTests(context.Background(), nil)
		require.Nil(t, tds)
		require.NotNil(t, err)
		require.Equal(t, "failed to decode PEM block containing the key", err.Error())

	})
}

func TestNewDefaultSelectTestsReq(t *testing.T) {
	t.Run("defaults to master branch behavior", func(t *testing.T) {
		// No TC_BUILD_BRANCH env var set
		_ = os.Unsetenv("TC_BUILD_BRANCH")

		req := NewDefaultSelectTestsReq(spec.Azure, "ut suite")
		require.Equal(t, spec.Azure, req.cloud)
		require.Equal(t, "ut suite", req.suite)
		require.Equal(t, defaultForPastDays, req.forPastDays)
		require.Equal(t, defaultFirstRunOn, req.firstRunOn, "master branch should use defaultFirstRunOn")
		require.Equal(t, defaultLastRunOn, req.lastRunOn)
		require.Equal(t, "master", req.currentBranch)
	})

	t.Run("non-master branch disables firstRunOn", func(t *testing.T) {
		// Set to a release branch
		_ = os.Setenv("TC_BUILD_BRANCH", "release-24.3")
		defer func() { _ = os.Unsetenv("TC_BUILD_BRANCH") }()

		req := NewDefaultSelectTestsReq(spec.GCE, "nightly")
		require.Equal(t, spec.GCE, req.cloud)
		require.Equal(t, "nightly", req.suite)
		require.Equal(t, 0, req.firstRunOn, "non-master branch should disable firstRunOn")
		require.Equal(t, "release-24.3", req.currentBranch)
	})
}

func Test_getSFCreds(t *testing.T) {
	_ = os.Unsetenv(sfUsernameEnv)
	_ = os.Unsetenv(sfPrivateKey)
	t.Run("expect username env failure", func(t *testing.T) {
		u, p, e := getSFCreds()
		require.Empty(t, u)
		require.Empty(t, p)
		require.NotNil(t, e)
		require.Equal(t, fmt.Sprintf("environment variable %s is not set", sfUsernameEnv), e.Error())
	})
	t.Run("expect private key file env failure", func(t *testing.T) {
		_ = os.Setenv(sfUsernameEnv, "dummy_user")
		u, p, e := getSFCreds()
		require.Empty(t, u)
		require.Empty(t, p)
		require.NotNil(t, e)
		require.Equal(t, fmt.Sprintf("environment variable %s is not set", sfPrivateKey), e.Error())
	})
	t.Run("expect no failure", func(t *testing.T) {
		_ = os.Setenv(sfUsernameEnv, "dummy_user")
		key := createPrivateKey(t)
		_ = os.Setenv(sfPrivateKey, key)
		u, p, e := getSFCreds()
		require.Equal(t, os.Getenv(sfUsernameEnv), u)
		require.Equal(t, key, p)
		require.Nil(t, e)
	})
}

// create a private key for testing
func createPrivateKey(t *testing.T) string {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.Nil(t, err)
	// Convert private key to PKCS#1 ASN.1 PEM
	pemBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}
	return string(pem.EncodeToMemory(pemBlock))
}
