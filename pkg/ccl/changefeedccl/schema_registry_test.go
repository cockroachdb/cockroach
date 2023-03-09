// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConfluentSchemaRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("errors with no scheme", func(t *testing.T) {
		_, err := newConfluentSchemaRegistry("justsomestring", nil, nil)
		require.Error(t, err)
	})
	t.Run("errors with unsupported scheme", func(t *testing.T) {
		url := "gopher://myhost"
		_, err := newConfluentSchemaRegistry(url, nil, nil)
		require.Error(t, err)
	})
}

type mockExternalConnectionProvider map[string]string

func (m mockExternalConnectionProvider) lookup(name string) (string, error) {
	v, ok := m[name]
	if !ok {
		return v, errors.New("not found")
	}
	return v, nil
}

func TestConfluentSchemaRegistryExternalConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regServer := cdctest.StartTestSchemaRegistry()
	defer regServer.Close()

	m := mockExternalConnectionProvider{
		"good_endpoint": regServer.URL(),
		"bad_endpoint":  "http://bad",
	}

	reg, err := newConfluentSchemaRegistry("external://good_endpoint", m, nil)
	require.NoError(t, err)
	require.NoError(t, reg.Ping(context.Background()))

	// We can load a bad endpoint, but ping should fail.
	reg, err = newConfluentSchemaRegistry("external://bad_endpoint", m, nil)
	require.NoError(t, err)
	require.Error(t, reg.Ping(context.Background()))

	_, err = newConfluentSchemaRegistry("external://no_endpoint", m, nil)
	require.Error(t, err)

}

func TestConfluentSchemaRegistryPing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regServer := cdctest.StartTestSchemaRegistry()
	defer regServer.Close()

	t.Run("ping works when all is well", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL(), nil, nil)
		require.NoError(t, err)
		require.NoError(t, reg.Ping(context.Background()))
	})
	t.Run("ping does not error from HTTP 404", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL()+"/path-does-not-exist-but-we-do-not-care", nil, nil)
		require.NoError(t, err)
		require.NoError(t, reg.Ping(context.Background()), "Ping")
	})
	t.Run("Ping errors with bad host", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry("http://host-does-exist-and-we-care", nil, nil)
		require.NoError(t, err)
		require.Error(t, reg.Ping(context.Background()))
	})
}

// TestConfluentSchemaRegistryRetryMetrics verifies that we retry request to the schema registry
// at least 5 times.
func TestConfluentSchemaRegistryRetryMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regServer := cdctest.StartErrorTestSchemaRegistry(409)
	defer regServer.Close()

	sliMetrics, err := MakeMetrics(base.DefaultHistogramWindowInterval()).(*Metrics).AggMetrics.getOrCreateScope("")
	require.NoError(t, err)

	t.Run("ping works when all is well", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL(), nil, sliMetrics)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_, err = reg.RegisterSchemaForSubject(ctx, "subject1", "schema1")
		}()
		require.NoError(t, err)
		testutils.SucceedsSoon(t, func() error {
			if sliMetrics.SchemaRegistryRetries.Value() < 5 {
				return errors.New("insufficient retries detected")
			}
			return nil
		})
		cancel()
	})

}
