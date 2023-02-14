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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConfluentSchemaRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("errors with no scheme", func(t *testing.T) {
		_, err := newConfluentSchemaRegistry("justsomestring", nil)
		require.Error(t, err)
	})
	t.Run("errors with unsupported scheme", func(t *testing.T) {
		url := "gopher://myhost"
		_, err := newConfluentSchemaRegistry(url, nil)
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

	reg, err := newConfluentSchemaRegistry("external://good_endpoint", m)
	require.NoError(t, err)
	require.NoError(t, reg.Ping(context.Background()))

	// We can load a bad endpoint, but ping should fail.
	reg, err = newConfluentSchemaRegistry("external://bad_endpoint", m)
	require.NoError(t, err)
	require.Error(t, reg.Ping(context.Background()))

	reg, err = newConfluentSchemaRegistry("external://no_endpoint", m)
	require.Error(t, err)

}

func TestConfluentSchemaRegistryPing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regServer := cdctest.StartTestSchemaRegistry()
	defer regServer.Close()

	t.Run("ping works when all is well", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL(), nil)
		require.NoError(t, err)
		require.NoError(t, reg.Ping(context.Background()))
	})
	t.Run("ping does not error from HTTP 404", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL()+"/path-does-not-exist-but-we-do-not-care", nil)
		require.NoError(t, err)
		require.NoError(t, reg.Ping(context.Background()), "Ping")
	})
	t.Run("Ping errors with bad host", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry("http://host-does-exist-and-we-care", nil)
		require.NoError(t, err)
		require.Error(t, reg.Ping(context.Background()))
	})
}
