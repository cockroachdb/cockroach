// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/stretchr/testify/require"
)

// marshalJWKS returns a serialized public JWK set containing a single RSA key
// with the given key ID.
func marshalJWKS(t *testing.T, kid string) []byte {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	key, err := jwk.Import(priv.Public())
	require.NoError(t, err)
	require.NoError(t, key.Set(jwk.KeyIDKey, kid))
	require.NoError(t, key.Set(jwk.AlgorithmKey, jwa.RS256()))
	set := jwk.NewSet()
	require.NoError(t, set.AddKey(key))
	b, err := json.Marshal(set)
	require.NoError(t, err)
	return b
}

// oidcTestServer is a fake identity provider that serves an OIDC discovery
// document and a JWKS endpoint, counting hits on each so tests can assert what
// the cache fetched.
type oidcTestServer struct {
	server        *httptest.Server
	discoveryHits atomic.Int32
	jwksHits      atomic.Int32
	currentJWKS   atomic.Pointer[[]byte]
}

func newOIDCTestServer(t *testing.T, initialKID string) *oidcTestServer {
	s := &oidcTestServer{}
	jwks := marshalJWKS(t, initialKID)
	s.currentJWKS.Store(&jwks)
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		s.discoveryHits.Add(1)
		_, _ = fmt.Fprintf(w, `{"jwks_uri": %q}`, s.server.URL+"/jwks")
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		s.jwksHits.Add(1)
		_, _ = w.Write(*s.currentJWKS.Load())
	})
	s.server = httptest.NewServer(mux)
	return s
}

// rotate replaces the served JWKS with one carrying a new key ID.
func (s *oidcTestServer) rotate(t *testing.T, kid string) {
	jwks := marshalJWKS(t, kid)
	s.currentJWKS.Store(&jwks)
}

func newTestJWKSCache(t *testing.T) (*JWKSCache, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cache, err := NewJWKSCache(ctx, httputil.NewClient())
	require.NoError(t, err)
	return cache, func() { cache.Close(); cancel() }
}

// TestJWKSCacheServesFromMemory verifies that after the first fetch, subsequent
// lookups are served from memory: neither the discovery document nor the JWKS is
// refetched (within the background refresh interval).
func TestJWKSCacheServesFromMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	srv := newOIDCTestServer(t, "kid1")
	defer srv.server.Close()
	cache, done := newTestJWKSCache(t)
	defer done()
	conf := mustParseJWTIssuersConf(srv.server.URL)

	set, err := cache.KeySet(ctx, &conf, srv.server.URL, "kid1")
	require.NoError(t, err)
	_, ok := set.LookupKeyID("kid1")
	require.True(t, ok, "expected kid1 in the fetched set")

	for i := 0; i < 5; i++ {
		_, err := cache.KeySet(ctx, &conf, srv.server.URL, "kid1")
		require.NoError(t, err)
	}

	require.EqualValues(t, 1, srv.jwksHits.Load(), "JWKS should be fetched once and served from cache")
	require.EqualValues(t, 1, srv.discoveryHits.Load(), "discovery should be resolved once and cached")
}

// TestJWKSCacheRefreshesOnUnknownKID verifies that a token whose key ID is not
// in the cached set forces one refresh, and that repeated unknown-kid lookups
// within the cooldown do not stampede the identity provider.
func TestJWKSCacheRefreshesOnUnknownKID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	srv := newOIDCTestServer(t, "kid1")
	defer srv.server.Close()
	cache, done := newTestJWKSCache(t)
	defer done()
	conf := mustParseJWTIssuersConf(srv.server.URL)

	_, err := cache.KeySet(ctx, &conf, srv.server.URL, "kid1")
	require.NoError(t, err)
	require.EqualValues(t, 1, srv.jwksHits.Load())

	// A token with an unrecognized kid forces a single refresh.
	_, err = cache.KeySet(ctx, &conf, srv.server.URL, "surprise-kid")
	require.NoError(t, err)
	require.EqualValues(t, 2, srv.jwksHits.Load(), "unknown kid should force one refresh")

	// Further unknown-kid lookups within the cooldown must not refetch.
	for i := 0; i < 3; i++ {
		_, err = cache.KeySet(ctx, &conf, srv.server.URL, "surprise-kid")
		require.NoError(t, err)
	}
	require.EqualValues(t, 2, srv.jwksHits.Load(), "refresh should be rate-limited by the cooldown")
}

// TestJWKSCachePicksUpRotatedKey verifies that when the issuer rotates its
// signing key, the first token presenting the new key ID triggers a refresh that
// makes the new key available.
func TestJWKSCachePicksUpRotatedKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	srv := newOIDCTestServer(t, "kid1")
	defer srv.server.Close()
	cache, done := newTestJWKSCache(t)
	defer done()
	conf := mustParseJWTIssuersConf(srv.server.URL)

	set, err := cache.KeySet(ctx, &conf, srv.server.URL, "kid1")
	require.NoError(t, err)
	_, ok := set.LookupKeyID("kid1")
	require.True(t, ok)

	// The issuer rotates to kid2; the first token with kid2 must pick it up.
	srv.rotate(t, "kid2")
	set, err = cache.KeySet(ctx, &conf, srv.server.URL, "kid2")
	require.NoError(t, err)
	_, ok = set.LookupKeyID("kid2")
	require.True(t, ok, "rotated key kid2 should be available after unknown-kid refresh")
}

// TestJWKSCacheExplicitURI verifies that an explicitly configured jwks_uri is
// used directly, skipping OIDC discovery entirely.
func TestJWKSCacheExplicitURI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	srv := newOIDCTestServer(t, "kid1")
	defer srv.server.Close()
	cache, done := newTestJWKSCache(t)
	defer done()
	// Map the issuer directly to the JWKS endpoint.
	conf := mustParseJWTIssuersConf(fmt.Sprintf(
		`{"issuer_jwks_map": {%q: %q}}`, srv.server.URL, srv.server.URL+"/jwks"))

	set, err := cache.KeySet(ctx, &conf, srv.server.URL, "kid1")
	require.NoError(t, err)
	_, ok := set.LookupKeyID("kid1")
	require.True(t, ok)
	require.EqualValues(t, 0, srv.discoveryHits.Load(), "explicit jwks_uri should skip discovery")
	require.EqualValues(t, 1, srv.jwksHits.Load())
}

// TestJWKSCacheInitialFetchFailureRespectsCallerContext verifies that racing
// registrations wait for readiness under their own contexts and that a failed
// first fetch does not poison later lookups with an uncancellable wait.
func TestJWKSCacheInitialFetchFailureRespectsCallerContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(requestStarted) })
		<-releaseRequest
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	cache, done := newTestJWKSCache(t)
	defer done()
	conf := mustParseJWTIssuersConf(fmt.Sprintf(
		`{"issuer_jwks_map": {%q: %q}}`, srv.URL, srv.URL))

	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() {
		_, err := cache.KeySet(firstCtx, &conf, srv.URL, "kid1")
		firstResult <- err
	}()
	<-requestStarted

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelSecond()
	_, err := cache.KeySet(secondCtx, &conf, srv.URL, "kid1")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(releaseRequest)
	select {
	case err := <-firstResult:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("first lookup did not respect its context")
	}

	thirdCtx, cancelThird := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelThird()
	_, err = cache.KeySet(thirdCtx, &conf, srv.URL, "kid1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestJWKSCacheInitialFetchUsesHTTPClientTimeout verifies that a cold-cache
// lookup is bounded by the configured HTTP client timeout even when its caller
// has no deadline, as is the case on the pgwire authentication path.
func TestJWKSCacheInitialFetchUsesHTTPClientTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	const clientTimeout = 100 * time.Millisecond
	cache, err := NewJWKSCache(context.Background(), httputil.NewClient(
		httputil.WithClientTimeout(clientTimeout),
		httputil.WithDialerTimeout(clientTimeout),
	))
	require.NoError(t, err)
	defer cache.Close()
	conf := mustParseJWTIssuersConf(fmt.Sprintf(
		`{"issuer_jwks_map": {%q: %q}}`, srv.URL, srv.URL))

	result := make(chan error, 1)
	go func() {
		_, err := cache.KeySet(context.Background(), &conf, srv.URL, "kid1")
		result <- err
	}()
	select {
	case err := <-result:
		var timeoutErr *timeutil.TimeoutError
		require.ErrorAs(t, err, &timeoutErr)
	case <-time.After(2 * time.Second):
		t.Fatal("cold-cache lookup did not respect the HTTP client timeout")
	}
}

// TestJWKSCacheInFlightLookupCanceledOnClose verifies that closing a cache
// cancels a first fetch that is waiting for readiness, matching the race where
// a configuration change replaces a cache during an in-flight login.
func TestJWKSCacheInFlightLookupCanceledOnClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requestStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(requestStarted) })
		select {
		case <-r.Context().Done():
		case <-releaseHandler:
		}
	}))
	defer srv.Close()
	defer close(releaseHandler)

	cache, done := newTestJWKSCache(t)
	defer done()
	conf := mustParseJWTIssuersConf(fmt.Sprintf(
		`{"issuer_jwks_map": {%q: %q}}`, srv.URL, srv.URL))
	result := make(chan error, 1)
	go func() {
		_, err := cache.KeySet(context.Background(), &conf, srv.URL, "kid1")
		result <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("cold-cache fetch did not reach the identity provider")
	}
	cache.Close()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight cold-cache lookup was not canceled by Close")
	}
}

// TestJWKSCacheLookupAfterClose verifies that a caller which loaded a cache
// immediately before replacement cannot block in the stopped controller.
func TestJWKSCacheLookupAfterClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	srv := newOIDCTestServer(t, "kid1")
	defer srv.server.Close()
	cache, done := newTestJWKSCache(t)
	defer done()
	conf := mustParseJWTIssuersConf(fmt.Sprintf(
		`{"issuer_jwks_map": {%q: %q}}`, srv.server.URL, srv.server.URL+"/jwks"))

	_, err := cache.KeySet(context.Background(), &conf, srv.server.URL, "kid1")
	require.NoError(t, err)
	cache.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = cache.KeySet(ctx, &conf, srv.server.URL, "kid1")
	require.ErrorIs(t, err, context.Canceled)
}
