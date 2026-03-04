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
			{"t1", "no", "12345", "no"},
			{"t2", "no", "12345", "no"},
			{"t3", "no", "12345", "yes"},
			{"t4", "no", "12345", "no"},
			{"t5", "yes", "12345", "no"},
			{"t6", "yes", "12345", "no"},
			{"t7", "yes", "12345", "no"},
			{"t8", "yes", "12345", "no"},
			{"t9", "yes", "12345", "no"},
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
		}
	})
	t.Run("first run with no snowflake history", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		mock.ExpectPrepare(regexp.QuoteMeta(PreparedQuery))
		// Return empty result set (no Snowflake history at all - first run scenario)
		rows := sqlmock.NewRows(AllRows)
		mock.ExpectQuery(regexp.QuoteMeta(PreparedQuery)).WillReturnRows(rows)

		// All tests available
		allTestNames := []string{"acceptance", "backup", "import", "kv", "schemachange",
			"sqlsmith", "tpcc", "ycsb", "restore", "decommission"}
		selectPct := 0.35
		tds, err := CategoriseTests(context.Background(), &SelectTestsReq{
			forPastDays:  1,
			firstRunOn:   2,
			lastRunOn:    3,
			cloud:        spec.AWS,
			suite:        "unit test",
			allTestNames: allTestNames,
			selectPct:    selectPct,
		})
		require.Nil(t, err)
		require.NotNil(t, tds)
		// Should have all 10 tests
		require.Equal(t, len(allTestNames), len(tds))

		// Verify all tests have zero duration and no preemption (no history)
		for _, td := range tds {
			require.Equal(t, int64(0), td.AvgDurationInMillis)
			require.False(t, td.LastFailureIsPreempt)
		}

		// Verify that all test names are present in the results
		testNamesInResults := make(map[string]bool)
		for _, td := range tds {
			testNamesInResults[td.Name] = true
		}
		for _, name := range allTestNames {
			require.True(t, testNamesInResults[name], "Test %s should be in results", name)
		}

		// Count selected tests and verify it's approximately the right percentage
		selectedCount := 0
		for _, td := range tds {
			if td.Selected {
				selectedCount++
			}
		}
		expectedCount := int(float64(len(allTestNames)) * selectPct)
		require.Equal(t, expectedCount, selectedCount,
			"Should select approximately %d%% of tests", int(selectPct*100))
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
	testNames := []string{"test1", "test2", "test3"}
	selectPct := 0.35
	req := NewDefaultSelectTestsReq(spec.Azure, "ut suite", testNames, selectPct)
	require.Equal(t, spec.Azure, req.cloud)
	require.Equal(t, "ut suite", req.suite)
	require.Equal(t, defaultForPastDays, req.forPastDays)
	require.Equal(t, defaultFirstRunOn, req.firstRunOn)
	require.Equal(t, defaultLastRunOn, req.lastRunOn)
	require.Equal(t, testNames, req.allTestNames)
	require.Equal(t, selectPct, req.selectPct)
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
