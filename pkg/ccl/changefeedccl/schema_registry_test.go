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
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConfluentSchemaRegistryPing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regServer := cdctest.StartTestSchemaRegistry()
	defer regServer.Close()

	t.Run("ping works when all is well", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL())
		require.NoError(t, err)
		require.NoError(t, reg.Ping(context.Background()))
	})
	t.Run("ping does not error from HTTP 404", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry(regServer.URL() + "/path-does-not-exist-but-we-do-not-care")
		require.NoError(t, err)
		require.NoError(t, reg.Ping(context.Background()), "Ping")
	})
	t.Run("Ping errors with bad host", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry("http://host-does-exist-and-we-care")
		require.NoError(t, err)
		require.Error(t, reg.Ping(context.Background()))
	})
	t.Run("Ping errors with no scheme", func(t *testing.T) {
		reg, err := newConfluentSchemaRegistry("justsomestring")
		require.NoError(t, err)                          // url.Parse is everyone's friend
		require.Error(t, reg.Ping(context.Background())) // we aren't
	})
	t.Run("Ping errors with bad scheme", func(t *testing.T) {
		url, err := url.Parse(regServer.URL())
		require.NoError(t, err)
		url.Scheme = "gopher://"
		reg, err := newConfluentSchemaRegistry(url.String())
		require.NoError(t, err)
		require.Error(t, reg.Ping(context.Background()))
	})
}
