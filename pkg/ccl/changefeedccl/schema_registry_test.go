// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
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

	t.Run("configure timeout", func(t *testing.T) {
		regServer := cdctest.StartTestSchemaRegistry()
		defer regServer.Close()
		r, err := newConfluentSchemaRegistry(regServer.URL(), nil, nil)
		require.NoError(t, err)
		getTimeout := func(r schemaRegistry) time.Duration {
			return r.(*schemaRegistryWithCache).base.(*confluentSchemaRegistry).client.Timeout
		}
		require.Equal(t, defaultSchemaRegistryTimeout, getTimeout(r))

		// add explicit timeout param.
		u, err := url.Parse(regServer.URL())
		require.NoError(t, err)
		values := u.Query()
		values.Set(timeoutParam, "42ms")
		u.RawQuery = values.Encode()
		r, err = newConfluentSchemaRegistry(u.String(), nil, nil)
		require.NoError(t, err)
		require.Equal(t, 42*time.Millisecond, getTimeout(r))
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

func TestConfluentSchemaRegistrySharedCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regServer := cdctest.StartTestSchemaRegistry()
	defer regServer.Close()
	require.Equal(t, 0, regServer.RegistrationCount())

	var wg sync.WaitGroup

	// Multiple registrations of the same schema hit a shared cache.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			r, err := newConfluentSchemaRegistry(regServer.URL(), nil, nil)
			require.NoError(t, err)
			_, err = r.RegisterSchemaForSubject(context.Background(), "subject1", "schema")
			require.NoError(t, err)
			wg.Done()

		}()
	}
	wg.Wait()
	require.Equal(t, 1, regServer.RegistrationCount())

	// Registrations of different schemas don't share a cache, even if the subject is the same.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			r, err := newConfluentSchemaRegistry(regServer.URL(), nil, nil)
			require.NoError(t, err)
			_, err = r.RegisterSchemaForSubject(context.Background(), "subject1", fmt.Sprintf("schema1%d", i))
			require.NoError(t, err)
			wg.Done()

		}(i)
	}
	wg.Wait()
	require.Equal(t, 11, regServer.RegistrationCount())

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

	sliMetrics, err := MakeMetrics(base.DefaultHistogramWindowInterval(), cidr.NewTestLookup()).(*Metrics).AggMetrics.getOrCreateScope("")
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
		require.EqualValues(t, 0, sliMetrics.SchemaRegistrations.Value())
		cancel()
	})

}
