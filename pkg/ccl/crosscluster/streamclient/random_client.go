// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// RandomGenScheme is the URI scheme used to create a test load.
const RandomGenScheme = "randomgen"

// InterceptFn is a function that will intercept events emitted by
// an InterceptableStreamClient
type InterceptFn func(event crosscluster.Event, spec SubscriptionToken)

// DialInterceptFn is a function that will intercept Dial calls made to an
// InterceptableStreamClient
type DialInterceptFn func(streamURL *url.URL) error

// HeartbeatInterceptFn is a function that will intercept calls to a client's
// Heartbeat.
type HeartbeatInterceptFn func(timestamp hlc.Timestamp)

// SSTableMakerFn is a function that generates RangeFeedSSTable event
// with a given list of roachpb.KeyValue.
type SSTableMakerFn func(keyValues []roachpb.KeyValue) kvpb.RangeFeedSSTable

type RandomClient interface {
	Client

	// RegisterInterception registers a interceptor to be called after
	// an event is emitted from the client.
	RegisterInterception(fn InterceptFn)

	// RegisterDialInterception registers a interceptor to be called
	// whenever Dial is called on the client.
	RegisterDialInterception(fn DialInterceptFn)

	// RegisterHeartbeatInterception registers an interceptor to be called
	// whenever Heartbeat is called on the client.
	RegisterHeartbeatInterception(fn HeartbeatInterceptFn)

	// RegisterSSTableGenerator registers a functor to be called
	// whenever an SSTable event is to be generated.
	RegisterSSTableGenerator(fn SSTableMakerFn)

	// ClearInterceptors clears all registered interceptors on the client.
	ClearInterceptors()

	URL() string
}

var (
	RandomGenClientBuilder func(*url.URL, descs.DB) (Client, error) = func(*url.URL, descs.DB) (Client, error) {
		return nil, errors.AssertionFailedf("to use the randomgen scheme include pkg/ccl/crosscluster/streamclient/randclient")
	}

	// GetRandomStreamClientSingletonForTesting returns the singleton instance of
	// the client. This is to be used in testing, when interceptors can be
	// registered on the client to observe events.
	GetRandomStreamClientSingletonForTesting func() RandomClient = func() RandomClient {
		panic("to use the randomgen scheme include pkg/ccl/crosscluster/streamclient/randclient")
	}
)
