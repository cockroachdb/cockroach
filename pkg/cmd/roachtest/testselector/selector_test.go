// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testselector

import (
	"context"
	gosql "database/sql"
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
	_ = os.Unsetenv(sfPasswordEnv)
	t.Run("expect getConnect to fail due to missing SFUSER env", func(t *testing.T) {
		SqlConnectorFunc = nil
		tds, err := CategoriseTests(context.Background(), nil)
		require.Nil(t, tds)
		require.NotNil(t, err)
		require.Equal(t, "environment variable SFUSER is not set", err.Error())
	})
	_ = os.Setenv(sfUsernameEnv, "dummy_user")
	_ = os.Setenv(sfPasswordEnv, "dummy_password")
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
			ForPastDays: 1,
			FirstRunOn:  2,
			LastRunOn:   3,
			Cloud:       spec.AWS,
			Suite:       "unit test",
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
			ForPastDays: 1,
			FirstRunOn:  2,
			LastRunOn:   3,
			Cloud:       spec.AWS,
			Suite:       "unit test",
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
}

func TestNewDefaultSelectTestsReq(t *testing.T) {
	req := NewDefaultSelectTestsReq(spec.Azure, "ut suite")
	require.Equal(t, spec.Azure, req.Cloud)
	require.Equal(t, "ut suite", req.Suite)
	require.Equal(t, defaultForPastDays, req.ForPastDays)
	require.Equal(t, defaultFirstRunOn, req.FirstRunOn)
	require.Equal(t, defaultLastRunOn, req.LastRunOn)
}

func Test_getSFCreds(t *testing.T) {
	_ = os.Unsetenv(sfUsernameEnv)
	_ = os.Unsetenv(sfPasswordEnv)
	t.Run("expect username env failure", func(t *testing.T) {
		u, p, e := getSFCreds()
		require.Empty(t, u)
		require.Empty(t, p)
		require.NotNil(t, e)
		require.Equal(t, fmt.Sprintf("environment variable %s is not set", sfUsernameEnv), e.Error())
	})
	t.Run("expect password env failure", func(t *testing.T) {
		_ = os.Setenv(sfUsernameEnv, "dummy_user")
		u, p, e := getSFCreds()
		require.Empty(t, u)
		require.Empty(t, p)
		require.NotNil(t, e)
		require.Equal(t, fmt.Sprintf("environment variable %s is not set", sfPasswordEnv), e.Error())
	})
	t.Run("expect no failure", func(t *testing.T) {
		_ = os.Setenv(sfUsernameEnv, "dummy_user")
		_ = os.Setenv(sfPasswordEnv, "dummy_password")
		u, p, e := getSFCreds()
		require.Equal(t, os.Getenv(sfUsernameEnv), u)
		require.Equal(t, os.Getenv(sfPasswordEnv), p)
		require.Nil(t, e)
	})
}
