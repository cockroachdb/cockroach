// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func Test_connExecutor_recordStatementLatencyMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type args struct {
		flags               planFlags
		automaticRetryCount int
	}

	type expected struct {
		duration time.Duration
	}

	r := require.New(t)
	stmt, err := parser.ParseOne("SELECT * FROM Foo")
	r.NoError(err)

	query1 := &Statement{
		StmtNoConstants: "test1",
		Statement:       stmt,
	}

	query2 := &Statement{
		StmtNoConstants: "test2",
		Statement:       stmt,
	}

	testDuration := 2 * time.Millisecond
	tests := []struct {
		name string
		ex   *connExecutor
		args args
		exp  []expected
	}{
		{
			name: "No detail",
			ex:   executorStub(false),
			args: args{
				flags:               0,
				automaticRetryCount: 0,
			},
			exp: []expected{},
		},
		{
			name: "With detail",
			ex:   executorStub(true),
			args: args{
				flags:               0,
				automaticRetryCount: 0,
			},
			exp: []expected{
				{
					duration: testDuration,
				},
				{
					duration: testDuration,
				},
			},
		},
		{
			name: "With detail, distributed",
			ex:   executorStub(true),
			args: args{
				flags:               planFlagFullyDistributed,
				automaticRetryCount: 0,
			},
			exp: []expected{
				{
					duration: testDuration,
				},
				{
					duration: testDuration,
				},
			},
		},
		{
			name: "No metrics on retry",
			ex:   executorStub(true),
			args: args{
				flags:               0,
				automaticRetryCount: 1,
			},
			exp: []expected{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			tt.ex.recordStatementLatencyMetrics(query1, tt.args.flags, tt.args.automaticRetryCount, testDuration, testDuration)
			tt.ex.recordStatementLatencyMetrics(query2, tt.args.flags, tt.args.automaticRetryCount, testDuration, testDuration)
			m := tt.ex.metrics.EngineMetrics
			expectedFingerprints := int64(2)
			if tt.args.automaticRetryCount > 0 {
				expectedFingerprints = int64(0)
			}
			r.Equal(expectedFingerprints, m.StatementFingerprintCount.Count())

			sql := m.SQLExecLatencyDetail.ToPrometheusMetrics()
			r.Equal(len(tt.exp), len(sql))

			for i, expectation := range tt.exp {
				r.Equal(float64(expectation.duration), *sql[i].Histogram.SampleSum)
				r.Equal(uint64(2/len(tt.exp)), *sql[i].Histogram.SampleCount)
			}
		})
	}
}

func makeTestQuery(i int) (*Statement, error) {
	sql := fmt.Sprintf("SELECT * FROM Foo_%d", i)
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return nil, err
	}

	return &Statement{
		StmtNoConstants: formatStatementHideConstants(stmt.AST, tree.FmtCollapseLists|tree.FmtConstantsAsUnderscores),
		Statement:       stmt,
	}, nil
}

func testDuration() time.Duration {
	return time.Duration(math.Abs(rand.NormFloat64())*10.0)*time.Millisecond + 2*time.Millisecond
}

func Benchmark_recordStatementLatencyMetrics(b *testing.B) {
	r := require.New(b)

	queries := make([]*Statement, 75)
	durations := make([]time.Duration, len(queries))
	for i := 0; i < len(queries); i++ {
		q, err := makeTestQuery(i)
		r.NoError(err)
		queries[i] = q

		durations[i] = testDuration()
	}

	noDetail := executorStub(false)
	b.Run("NoDetail", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < len(queries); j++ {
				d := durations[j]
				noDetail.recordStatementLatencyMetrics(queries[j], 0, 0, d, d)
			}
		}
	})

	withDetail := executorStub(true)
	b.Run("With Detail", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < len(queries); j++ {
				d := durations[j]
				withDetail.recordStatementLatencyMetrics(queries[j], 0, 0, d, d)
			}
		}
	})
}

func executorStub(detailEnabled bool) *connExecutor {
	ex := &connExecutor{}
	ex.server = &Server{
		cfg: &ExecutorConfig{
			Settings: cluster.MakeTestingClusterSettings(),
		},
	}
	metrics := makeMetrics(false, &ex.server.cfg.Settings.SV)
	ex.metrics = &metrics

	detailedLatencyMetrics.Override(context.Background(), &ex.server.cfg.Settings.SV, detailEnabled)

	return ex
}
