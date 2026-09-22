// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/require"
)

// testOIDCServer is a minimal OpenID provider whose responses can be mutated
// between logins by the cache tests.
type testOIDCServer struct {
	server *httptest.Server

	mu struct {
		sync.Mutex
		clock            func() time.Time
		discoveryBody    []byte
		discoveryHeaders http.Header
		discoveryStatus  int
		discoveryHits    int
		jwksBody         []byte
		jwksHeaders      http.Header
		jwksStatus       int
		jwksHits         int
		jwksCacheControl []string
	}
}

func newTestOIDCServer() *testOIDCServer {
	s := &testOIDCServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration",
		func(w http.ResponseWriter, r *http.Request) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.mu.discoveryHits++
			s.setDateHeader(w)
			if s.mu.discoveryStatus != 0 {
				w.WriteHeader(s.mu.discoveryStatus)
			}
			writeTestOIDCResponse(w, s.mu.discoveryHeaders, s.mu.discoveryBody)
		})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.jwksHits++
		s.mu.jwksCacheControl = append(s.mu.jwksCacheControl, r.Header.Get("Cache-Control"))
		s.setDateHeader(w)
		if s.mu.jwksStatus != 0 {
			w.WriteHeader(s.mu.jwksStatus)
		}
		writeTestOIDCResponse(w, s.mu.jwksHeaders, s.mu.jwksBody)
	})
	s.server = httptest.NewServer(mux)
	return s
}

// Close shuts down the test provider. Callers defer it rather than using
// t.Cleanup so that the server's goroutine is gone before leaktest runs
// (deferred calls run before test cleanup functions).
func (s *testOIDCServer) Close() { s.server.Close() }

// setDateHeader overrides the server-generated Date header with the test's
// clock, so a manual clock used to advance cache time stays consistent with
// the response's apparent age. The caller must hold s.mu.
func (s *testOIDCServer) setDateHeader(w http.ResponseWriter) {
	if s.mu.clock != nil {
		w.Header().Set("Date", s.mu.clock().UTC().Format(http.TimeFormat))
	}
}

// setClock makes the server stamp response Date headers from the given clock.
func (s *testOIDCServer) setClock(clock func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.clock = clock
}

func writeTestOIDCResponse(w http.ResponseWriter, headers http.Header, body []byte) {
	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	_, _ = w.Write(body)
}

// pointDiscoveryAtJWKS makes the discovery document advertise the server's own
// JWKS endpoint.
func (s *testOIDCServer) pointDiscoveryAtJWKS(headers http.Header) {
	s.pointDiscoveryAt(s.server.URL+"/jwks", headers)
}

// pointDiscoveryAt makes the discovery document advertise the given JWKS URI.
func (s *testOIDCServer) pointDiscoveryAt(jwksURI string, headers http.Header) {
	s.setDiscovery([]byte(fmt.Sprintf(`{"issuer": %q, "jwks_uri": %q}`,
		s.server.URL, jwksURI)), headers)
}

func (s *testOIDCServer) setDiscovery(body []byte, headers http.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.discoveryBody, s.mu.discoveryHeaders = body, headers
}

func (s *testOIDCServer) setJWKS(body string, headers http.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.jwksBody, s.mu.jwksHeaders = []byte(body), headers
}

func (s *testOIDCServer) setJWKSStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.jwksStatus = status
}

func (s *testOIDCServer) hits() (discovery, jwks int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.discoveryHits, s.mu.jwksHits
}

func (s *testOIDCServer) assertHits(t *testing.T, discovery, jwks int) {
	t.Helper()
	actualDiscovery, actualJWKS := s.hits()
	require.Equal(t, discovery, actualDiscovery, "discovery request count")
	require.Equal(t, jwks, actualJWKS, "jwks request count")
}

func (s *testOIDCServer) lastJWKSRequestCacheControl() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.jwksCacheControl) == 0 {
		return ""
	}
	return s.mu.jwksCacheControl[len(s.mu.jwksCacheControl)-1]
}

func cacheMaxAgeHeader(seconds int) http.Header {
	return http.Header{"Cache-Control": []string{fmt.Sprintf("max-age=%d", seconds)}}
}

// newCacheTestKeys returns a signing key and a JWK set containing only that
// key's public half, suitable for serving over JWKS.
func newCacheTestKeys(t *testing.T, keyID string) (jwk.Key, jwk.Set) {
	t.Helper()
	key := createRSAKey(t, keyID)
	return key, newCacheTestSet(t, key)
}

// newCacheTestSet returns a JWK set containing the public halves of keys.
func newCacheTestSet(t *testing.T, keys ...jwk.Key) jwk.Set {
	t.Helper()
	set := jwk.NewSet()
	for _, key := range keys {
		pub, err := key.PublicKey()
		require.NoError(t, err)
		require.NoError(t, set.AddKey(pub))
	}
	return set
}

// newCacheTestAuthenticator builds an auto-fetch JWT authenticator over a
// standalone test settings object, without starting a server.
func newCacheTestAuthenticator(t *testing.T, issuer string) (*jwtAuthenticator, *cluster.Settings) {
	t.Helper()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	JWTAuthEnabled.Override(ctx, &st.SV, true)
	JWTAuthIssuersConfig.Override(ctx, &st.SV, issuer)
	JWKSAutoFetchEnabled.Override(ctx, &st.SV, true)
	JWTAuthAudience.Override(ctx, &st.SV, audience1)
	verifier := ConfigureJWTAuth(ctx, log.AmbientContext{}, st, uuid.Nil)
	a, ok := verifier.(*jwtAuthenticator)
	require.True(t, ok)
	return a, st
}

func cacheTestIdentMap(t *testing.T) *identmap.Conf {
	t.Helper()
	identMap, err := identmap.From(strings.NewReader(""))
	require.NoError(t, err)
	return identMap
}

func cacheTestLogin(t *testing.T, a *jwtAuthenticator, st *cluster.Settings, token []byte) error {
	t.Helper()
	_, err := a.ValidateJWTLogin(
		context.Background(), st, username.MakeSQLUsernameFromPreNormalizedString(username1),
		token, cacheTestIdentMap(t))
	return err
}

func cacheTestToken(t *testing.T, key jwk.Key, issuer string) []byte {
	t.Helper()
	return createJWT(t, username1, audience1, issuer, timeutil.Now().Add(time.Hour),
		key, jwa.RS256, "", "")
}

// useManualClock installs a manual clock on the authenticator so tests can
// advance cache time deterministically without sleeping.
func useManualClock(a *jwtAuthenticator) *timeutil.ManualTime {
	manual := timeutil.NewManualTime(timeutil.Now())
	a.clock = manual.Now
	return manual
}

func TestRemoteJWKSCacheFreshness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	now := timeutil.Now()
	const maxAge = 300 * time.Second

	for _, tc := range []struct {
		name       string
		headers    http.Header
		evaluateAt time.Duration
		wantFresh  bool
	}{
		{
			name:       "max-age is fresh",
			headers:    http.Header{"Cache-Control": []string{"max-age=300"}},
			evaluateAt: maxAge - time.Second,
			wantFresh:  true,
		},
		{
			name:       "max-age expires",
			headers:    http.Header{"Cache-Control": []string{"max-age=300"}},
			evaluateAt: maxAge + time.Second,
			wantFresh:  false,
		},
		{
			name: "Age reduces the remaining lifetime",
			headers: http.Header{
				"Cache-Control": []string{"max-age=300"},
				"Age":           []string{"300"},
			},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name: "Expires falls back to Date",
			headers: http.Header{
				"Date":    []string{now.UTC().Format(http.TimeFormat)},
				"Expires": []string{now.Add(maxAge).UTC().Format(http.TimeFormat)},
			},
			evaluateAt: maxAge - time.Second,
			wantFresh:  true,
		},
		{
			name:       "invalid Expires is already expired",
			headers:    http.Header{"Expires": []string{"0"}},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name:       "missing freshness is stale",
			headers:    http.Header{},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name:       "no-store forbids reuse",
			headers:    http.Header{"Cache-Control": []string{"max-age=300, no-store"}},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name: "no-store on a later header line forbids reuse",
			headers: http.Header{
				"Cache-Control": []string{"max-age=300", "no-store"},
			},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name:       "no-cache forbids reuse",
			headers:    http.Header{"Cache-Control": []string{"max-age=300, no-cache"}},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name:       "must-revalidate is fresh until expiry",
			headers:    http.Header{"Cache-Control": []string{"max-age=300, must-revalidate"}},
			evaluateAt: time.Second,
			wantFresh:  true,
		},
		{
			name:       "duplicate max-age is stale",
			headers:    http.Header{"Cache-Control": []string{"max-age=300, max-age=60"}},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name:       "invalid max-age is stale",
			headers:    http.Header{"Cache-Control": []string{"max-age=abc"}},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name: "overflowing negative max-age is stale",
			headers: http.Header{
				"Cache-Control": []string{"max-age=-9223372036854775809"},
			},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name: "overflowing max-age is clamped to a large lifetime",
			headers: http.Header{
				"Cache-Control": []string{"max-age=99999999999999999999"},
			},
			evaluateAt: 0,
			wantFresh:  true,
		},
		{
			name: "max-age overrides Expires",
			headers: http.Header{
				"Cache-Control": []string{"max-age=1"},
				"Expires":       []string{now.Add(time.Hour).UTC().Format(http.TimeFormat)},
			},
			evaluateAt: 2 * time.Second,
			wantFresh:  false,
		},
		{
			name:       "quoted max-age is accepted",
			headers:    http.Header{"Cache-Control": []string{`max-age="300"`}},
			evaluateAt: time.Second,
			wantFresh:  true,
		},
		{
			name: "ancient Date cannot wrap into freshness",
			headers: http.Header{
				"Cache-Control": []string{"max-age=300"},
				"Date":          []string{"Sat, 01 Jan 1600 00:00:00 GMT"},
			},
			evaluateAt: time.Hour,
			wantFresh:  false,
		},
		{
			name: "quoted extension data cannot manufacture freshness",
			headers: http.Header{
				"Cache-Control": []string{`extension="ignored,max-age=86400,ignored"`},
			},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name: "quoted commas do not hide real directives",
			headers: http.Header{
				"Cache-Control": []string{`foo="a,b", max-age=300`},
			},
			evaluateAt: time.Second,
			wantFresh:  true,
		},
		{
			name: "conflicting Expires values are stale",
			headers: http.Header{
				"Expires": []string{
					now.Add(time.Hour).UTC().Format(http.TimeFormat),
					now.Add(-time.Hour).UTC().Format(http.TimeFormat),
				},
			},
			evaluateAt: 0,
			wantFresh:  false,
		},
		{
			name: "unterminated quoting is unusable freshness",
			headers: http.Header{
				"Cache-Control": []string{`max-age=300, foo="unterminated`},
			},
			evaluateAt: time.Second,
			wantFresh:  false,
		},
		{
			name: "Vary star forbids reuse",
			headers: http.Header{
				"Cache-Control": []string{"max-age=300"},
				"Vary":          []string{"*"},
			},
			evaluateAt: 0,
			wantFresh:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := parseResponseFreshness(tc.headers, now, now)
			require.Equal(t, tc.wantFresh, f.fresh(now.Add(tc.evaluateAt)))
		})
	}
}

func TestRemoteJWKSCacheAgeAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	requestTime := timeutil.Now()
	responseTime := requestTime.Add(100 * time.Millisecond)
	f := parseResponseFreshness(http.Header{
		"Cache-Control": []string{"max-age=300"},
		"Age":           []string{"300"},
	}, requestTime, responseTime)
	// The Age value plus the response delay already exceeds the lifetime.
	require.False(t, f.fresh(responseTime))
	require.Equal(t, 300*time.Second+100*time.Millisecond, f.currentAge(responseTime))
}

func TestRemoteJWKSCacheReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
	key, set := newCacheTestKeys(t, "cache-kid-a")
	srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
	token := cacheTestToken(t, key, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)
	identMap := cacheTestIdentMap(t)

	// Concurrent logins from a cold cache coalesce onto one fetch: the
	// authenticator's lock serializes them, and all but the first observe a
	// fresh entry.
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = a.ValidateJWTLogin(ctx, st,
				username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	srv.assertHits(t, 1, 1)

	// Repeated logins reuse both the discovery document and the JWK set.
	for i := 0; i < 3; i++ {
		require.NoError(t, cacheTestLogin(t, a, st, token))
	}
	srv.assertHits(t, 1, 1)

	// VerifyAndExtractIssuer shares the same cache.
	issuer, _, err := a.VerifyAndExtractIssuer(ctx, st, token)
	require.NoError(t, err)
	require.Equal(t, srv.server.URL, issuer)
	srv.assertHits(t, 1, 1)
}

func TestRemoteJWKSCacheStaleDocumentsAndIndependence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name              string
		discoveryMaxAge   int
		jwksMaxAge        int
		wantDiscoveryHits int
		wantJWKSHits      int
	}{
		{name: "both stale", discoveryMaxAge: 0, jwksMaxAge: 0,
			wantDiscoveryHits: 2, wantJWKSHits: 2},
		{name: "fresh discovery does not pin stale jwks", discoveryMaxAge: 300, jwksMaxAge: 0,
			wantDiscoveryHits: 1, wantJWKSHits: 2},
		{name: "stale discovery does not refetch fresh jwks", discoveryMaxAge: 0, jwksMaxAge: 300,
			wantDiscoveryHits: 2, wantJWKSHits: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestOIDCServer()
			defer srv.Close()
			srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(tc.discoveryMaxAge))
			key, set := newCacheTestKeys(t, "cache-kid-a")
			srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(tc.jwksMaxAge))
			token := cacheTestToken(t, key, srv.server.URL)
			a, st := newCacheTestAuthenticator(t, srv.server.URL)

			for i := 0; i < 2; i++ {
				require.NoError(t, cacheTestLogin(t, a, st, token))
			}
			srv.assertHits(t, tc.wantDiscoveryHits, tc.wantJWKSHits)
		})
	}
}

// TestRemoteJWKSCacheJWKSURIChangeAbandonsOldSource verifies that when the
// issuer's discovery document starts advertising a different jwks_uri, the
// cached set from the old source is abandoned immediately, not reused.
func TestRemoteJWKSCacheJWKSURIChangeAbandonsOldSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srvOld := newTestOIDCServer()
	defer srvOld.Close()
	srvNew := newTestOIDCServer()
	defer srvNew.Close()

	keyOld, setOld := newCacheTestKeys(t, "cache-kid-old")
	keyNew, setNew := newCacheTestKeys(t, "cache-kid-new")
	srvOld.setJWKS(serializePublicKeySet(t, setOld), cacheMaxAgeHeader(300))
	srvNew.setJWKS(serializePublicKeySet(t, setNew), cacheMaxAgeHeader(300))
	// Discovery is always stale, so a URI change is observed on the next use.
	srvOld.pointDiscoveryAt(srvOld.server.URL+"/jwks", cacheMaxAgeHeader(0))

	tokenOld := cacheTestToken(t, keyOld, srvOld.server.URL)
	tokenNew := cacheTestToken(t, keyNew, srvOld.server.URL)
	a, st := newCacheTestAuthenticator(t, srvOld.server.URL)

	require.NoError(t, cacheTestLogin(t, a, st, tokenOld))
	srvOld.assertHits(t, 1, 1)

	// The issuer moves its JWKS to a new source. The old source's keys must
	// stop being trusted, and it must not be fetched again.
	srvOld.pointDiscoveryAt(srvNew.server.URL+"/jwks", cacheMaxAgeHeader(0))
	require.Error(t, cacheTestLogin(t, a, st, tokenOld))
	srvOld.assertHits(t, 2, 1)
	srvNew.assertHits(t, 0, 1)

	// The new source's keys are trusted. Discovery is still always-stale, so
	// the next use re-reads it, but the old JWKS source is never fetched
	// again.
	require.NoError(t, cacheTestLogin(t, a, st, tokenNew))
	srvOld.assertHits(t, 3, 1)
	srvNew.assertHits(t, 0, 1)
}

// TestRemoteJWKSCacheIneffectiveJWKSURIChangeNoResurrection verifies that a
// cached set is dropped when the issuer stops advertising its source even if
// the replacement fetch fails, so flipping discovery back does not reuse the
// old set without a fetch.
func TestRemoteJWKSCacheIneffectiveJWKSURIChangeNoResurrection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srvOld := newTestOIDCServer()
	defer srvOld.Close()
	srvNew := newTestOIDCServer()
	defer srvNew.Close()

	keyOld, setOld := newCacheTestKeys(t, "cache-kid-old")
	srvOld.setJWKS(serializePublicKeySet(t, setOld), cacheMaxAgeHeader(300))
	// The new source is unavailable, so replacing the cached set fails.
	srvNew.setJWKSStatus(http.StatusInternalServerError)
	srvOld.pointDiscoveryAt(srvOld.server.URL+"/jwks", cacheMaxAgeHeader(0))

	tokenOld := cacheTestToken(t, keyOld, srvOld.server.URL)
	a, st := newCacheTestAuthenticator(t, srvOld.server.URL)

	require.NoError(t, cacheTestLogin(t, a, st, tokenOld))
	srvOld.assertHits(t, 1, 1)

	// Discovery moves to the unavailable new source; authentication fails and
	// the old set must be dropped rather than kept for later reuse.
	srvOld.pointDiscoveryAt(srvNew.server.URL+"/jwks", cacheMaxAgeHeader(0))
	require.Error(t, cacheTestLogin(t, a, st, tokenOld))
	srvOld.assertHits(t, 2, 1)
	srvNew.assertHits(t, 0, 1)

	// Discovery flips back to the original source. The old cached set must not
	// be reused: it is fetched again.
	srvOld.pointDiscoveryAt(srvOld.server.URL+"/jwks", cacheMaxAgeHeader(0))
	require.NoError(t, cacheTestLogin(t, a, st, tokenOld))
	srvOld.assertHits(t, 3, 2)
	srvNew.assertHits(t, 0, 1)
}

// TestRemoteJWKSCacheCooldownSurvivesOrdinaryRefresh verifies that an ordinary
// refresh of an expired JWK set does not reset the unfamiliar-kid cooldown.
func TestRemoteJWKSCacheCooldownSurvivesOrdinaryRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
	keyA, setA := newCacheTestKeys(t, "cache-kid-a")
	srv.setJWKS(serializePublicKeySet(t, setA), cacheMaxAgeHeader(5))
	keyB, _ := newCacheTestKeys(t, "cache-kid-b")
	tokenA := cacheTestToken(t, keyA, srv.server.URL)
	tokenB := cacheTestToken(t, keyB, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)
	manual := useManualClock(a)
	srv.setClock(manual.Now)

	require.NoError(t, cacheTestLogin(t, a, st, tokenA))
	srv.assertHits(t, 1, 1)

	// An unfamiliar key ID buys one early refresh, arming the cooldown.
	manual.Advance(time.Second)
	require.Error(t, cacheTestLogin(t, a, st, tokenB))
	srv.assertHits(t, 1, 2)

	// The JWK set now expires, so the next attempt performs an ordinary
	// refresh, which must not reset the cooldown.
	manual.Advance(5 * time.Second)
	require.Error(t, cacheTestLogin(t, a, st, tokenB))
	srv.assertHits(t, 1, 3)

	// Still within 30s of the first early refresh: no second fetch may happen.
	require.Error(t, cacheTestLogin(t, a, st, tokenB))
	srv.assertHits(t, 1, 3)
}

// TestRemoteJWKSCacheInvalidJWKSURINotCached verifies that a discovery
// document advertising an unusable jwks_uri is rejected without caching, so
// recovery is immediate once the origin is repaired.
func TestRemoteJWKSCacheInvalidJWKSURINotCached(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(1))
	key, set := newCacheTestKeys(t, "cache-kid-a")
	srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
	token := cacheTestToken(t, key, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)
	manual := useManualClock(a)
	srv.setClock(manual.Now)

	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 1)

	// Discovery expires and the origin serves an unusable jwks_uri with a long
	// lifetime. The response must not be retained.
	manual.Advance(2 * time.Second)
	srv.pointDiscoveryAt(":", cacheMaxAgeHeader(86400))
	require.Error(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 2, 1)

	// The origin is repaired; recovery must be immediate because the invalid
	// document was not cached.
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(86400))
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 3, 1)
}

// TestRemoteJWKSCacheMalformedKeyNotCached verifies that a JWK set whose key
// material is malformed or unusable is rejected before caching, so even a
// response with a long lifetime cannot block recovery for a known key ID.
func TestRemoteJWKSCacheMalformedKeyNotCached(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	zeroCoordinate := strings.Repeat("A", 43) // 32 zero bytes in base64url
	for _, tc := range []struct {
		name string
		body string
	}{
		{
			name: "missing modulus",
			body: `{"keys":[{"kty":"RSA","kid":"cache-kid-a","n":"","e":"AQAB"}]}`,
		},
		{
			name: "zero modulus",
			body: `{"keys":[{"kty":"RSA","kid":"cache-kid-a","n":"AA","e":"AQAB"}]}`,
		},
		{
			name: "off-curve EC point",
			body: fmt.Sprintf(
				`{"keys":[{"kty":"EC","kid":"cache-kid-a","crv":"P-256","x":"%s","y":"%s"}]}`,
				zeroCoordinate, zeroCoordinate),
		},
		{
			name: "wrong-length Ed25519 key",
			body: `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"cache-kid-a","x":"AQ"}]}`,
		},
		{
			name: "malformed Ed25519 private key",
			body: `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"cache-kid-a","x":"AQ","d":"AQ"}]}`,
		},
		{
			name: "wrong-length X25519 key",
			body: `{"keys":[{"kty":"OKP","crv":"X25519","kid":"cache-kid-a","x":"AQ"}]}`,
		},
		{
			name: "wrong-length Ed448 key",
			body: `{"keys":[{"kty":"OKP","crv":"Ed448","kid":"cache-kid-a","x":"AQ"}]}`,
		},
		{
			name: "Ed25519 private key with a malformed seed",
			body: fmt.Sprintf(
				`{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"cache-kid-a","x":"%s","d":"AQ"}]}`,
				zeroCoordinate),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestOIDCServer()
			defer srv.Close()
			srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
			key, set := newCacheTestKeys(t, "cache-kid-a")
			srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(0))
			token := cacheTestToken(t, key, srv.server.URL)
			a, st := newCacheTestAuthenticator(t, srv.server.URL)

			require.NoError(t, cacheTestLogin(t, a, st, token))
			srv.assertHits(t, 1, 1)

			// The document parses, but the key cannot verify anything. It must
			// not replace the cached set even though it advertises a long
			// lifetime.
			srv.setJWKS(tc.body, cacheMaxAgeHeader(86400))
			require.Error(t, cacheTestLogin(t, a, st, token))
			srv.assertHits(t, 1, 2)

			// The origin is repaired; recovery is immediate because the
			// malformed response was not cached.
			srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
			require.NoError(t, cacheTestLogin(t, a, st, token))
			srv.assertHits(t, 1, 3)
		})
	}
}

// TestRemoteJWKSCachePrivateKeyMaterialStillVerifies verifies that a JWKS
// carrying private-key material (an issuer misconfiguration) remains usable:
// validation applies to the public half of the key rather than rejecting the
// document.
func TestRemoteJWKSCachePrivateKeyMaterialStillVerifies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
	key, _ := newCacheTestKeys(t, "cache-kid-a")
	privateSet := jwk.NewSet()
	require.NoError(t, privateSet.AddKey(key))
	srv.setJWKS(serializePublicKeySet(t, privateSet), cacheMaxAgeHeader(300))
	token := cacheTestToken(t, key, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)

	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 1)
}

// TestRemoteJWKSCacheUnmaterializableOKPKeyIsIgnored verifies that an OKP key
// whose curve the library cannot materialize (Ed448) does not invalidate a set
// whose other keys can still verify tokens.
func TestRemoteJWKSCacheUnmaterializableOKPKeyIsIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))

	_, edPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	edKey, err := jwk.FromRaw(edPriv)
	require.NoError(t, err)
	require.NoError(t, edKey.Set(jwk.KeyIDKey, "cache-kid-ed25519"))
	token := createJWT(t, username1, audience1, srv.server.URL, timeutil.Now().Add(time.Hour),
		edKey, jwa.EdDSA, "", "")

	ed448, err := jwk.ParseKey([]byte(fmt.Sprintf(
		`{"kty":"OKP","crv":"Ed448","kid":"cache-kid-ed448","x":"%s"}`,
		strings.Repeat("A", 76))))
	require.NoError(t, err)
	set := jwk.NewSet()
	edPub, err := edKey.PublicKey()
	require.NoError(t, err)
	require.NoError(t, set.AddKey(edPub))
	require.NoError(t, set.AddKey(ed448))
	srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))

	a, st := newCacheTestAuthenticator(t, srv.server.URL)
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 1)

	// The whole response was cached, including the unrelated Ed448 key.
	a.mu.RLock()
	entry := a.mu.jwksCache.jwks[srv.server.URL]
	a.mu.RUnlock()
	require.Equal(t, 2, entry.set.Len())
}

func TestRemoteJWKSCacheDirectives(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name         string
		cacheControl string
		wantRetained bool
	}{
		{name: "no-store drops the entry", cacheControl: "max-age=300, no-store"},
		{name: "no-cache retains but always refetches", cacheControl: "max-age=300, no-cache",
			wantRetained: true},
		{name: "missing freshness is stale", wantRetained: true},
		{name: "unusable quoting is not retained",
			cacheControl: `max-age=300, foo="unterminated`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestOIDCServer()
			defer srv.Close()
			var headers http.Header
			if tc.cacheControl != "" {
				headers = http.Header{"Cache-Control": []string{tc.cacheControl}}
			}
			srv.pointDiscoveryAtJWKS(headers)
			key, set := newCacheTestKeys(t, "cache-kid-a")
			srv.setJWKS(serializePublicKeySet(t, set), headers)
			token := cacheTestToken(t, key, srv.server.URL)
			a, st := newCacheTestAuthenticator(t, srv.server.URL)

			require.NoError(t, cacheTestLogin(t, a, st, token))
			a.mu.RLock()
			entry, jwksRetained := a.mu.jwksCache.jwks[srv.server.URL]
			discoveryRetained := len(a.mu.jwksCache.discovery) == 1
			a.mu.RUnlock()
			require.Equal(t, tc.wantRetained, jwksRetained)
			require.Equal(t, tc.wantRetained, discoveryRetained)
			if jwksRetained {
				require.False(t, entry.freshness.fresh(timeutil.Now()))
			}

			// Whatever was retained is never reused as-is: the next login
			// fetches again.
			require.NoError(t, cacheTestLogin(t, a, st, token))
			srv.assertHits(t, 2, 2)
		})
	}
}

func TestRemoteJWKSCacheRotationAndBoundedRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
	keyA, setA := newCacheTestKeys(t, "cache-kid-a")
	srv.setJWKS(serializePublicKeySet(t, setA), cacheMaxAgeHeader(300))
	tokenA := cacheTestToken(t, keyA, srv.server.URL)
	keyB, _ := newCacheTestKeys(t, "cache-kid-b")
	tokenB := cacheTestToken(t, keyB, srv.server.URL)
	keyC, _ := newCacheTestKeys(t, "cache-kid-c")
	tokenC := cacheTestToken(t, keyC, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)
	manual := useManualClock(a)
	srv.setClock(manual.Now)

	// A token signed by the cached key is verified without a refresh, and the
	// refresh request carries no cache override.
	require.NoError(t, cacheTestLogin(t, a, st, tokenA))
	srv.assertHits(t, 1, 1)
	require.Equal(t, "", srv.lastJWKSRequestCacheControl())

	// An unfamiliar key ID triggers one early refresh even though the cached
	// set is still fresh. The origin has not rotated yet, so verification
	// fails after the refresh.
	require.Error(t, cacheTestLogin(t, a, st, tokenB))
	srv.assertHits(t, 1, 2)
	require.Equal(t, "no-cache", srv.lastJWKSRequestCacheControl())

	// Further unfamiliar key IDs within the cooldown do not fetch again.
	require.Error(t, cacheTestLogin(t, a, st, tokenC))
	srv.assertHits(t, 1, 2)

	// The issuer rotates: the new set contains B. After the cooldown, the
	// unfamiliar key ID refreshes and authentication succeeds.
	srv.setJWKS(serializePublicKeySet(t, newCacheTestSet(t, keyA, keyB)),
		cacheMaxAgeHeader(300))
	manual.Advance(forcedJWKSRefreshCooldown + time.Second)
	require.NoError(t, cacheTestLogin(t, a, st, tokenB))
	srv.assertHits(t, 1, 3)

	// A successful refresh arms the cooldown too: another unknown key cannot
	// immediately provoke a fetch.
	require.Error(t, cacheTestLogin(t, a, st, tokenC))
	srv.assertHits(t, 1, 3)

	// Failed attempts arm the cooldown as well.
	srv.setJWKS(`{"keys": [`, cacheMaxAgeHeader(300))
	manual.Advance(forcedJWKSRefreshCooldown + time.Second)
	require.Error(t, cacheTestLogin(t, a, st, tokenC))
	srv.assertHits(t, 1, 4)
	require.Error(t, cacheTestLogin(t, a, st, tokenC))
	srv.assertHits(t, 1, 4)

	// A key ID present in the cached set never causes a fetch, even when the
	// signature is invalid: the bad signature is not evidence of rotation.
	srv.setJWKS(serializePublicKeySet(t, newCacheTestSet(t, keyA, keyB)),
		cacheMaxAgeHeader(300))
	require.NoError(t, cacheTestLogin(t, a, st, tokenB))
	impersonator := createRSAKey(t, "cache-kid-a")
	require.Error(t, cacheTestLogin(t, a, st, cacheTestToken(t, impersonator, srv.server.URL)))
	srv.assertHits(t, 1, 4)

	// Unfamiliar key IDs never accumulate per-key state.
	a.mu.RLock()
	discoveryEntries, jwksEntries := len(a.mu.jwksCache.discovery), len(a.mu.jwksCache.jwks)
	a.mu.RUnlock()
	require.Equal(t, 1, discoveryEntries)
	require.Equal(t, 1, jwksEntries)
}

func TestRemoteJWKSCacheFailsClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	user := username.MakeSQLUsernameFromPreNormalizedString(username1)
	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
	key, set := newCacheTestKeys(t, "cache-kid-a")
	srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(0))
	token := cacheTestToken(t, key, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)

	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 1)

	// The origin now fails. The previous keys are still present in the cache
	// but are stale, so they must not be used to authenticate.
	srv.setJWKSStatus(http.StatusInternalServerError)
	detailed, err := a.ValidateJWTLogin(ctx, st, user, token, cacheTestIdentMap(t))
	require.Error(t, err)
	require.Contains(t, detailed, "unable to fetch jwks")
	srv.assertHits(t, 1, 2)

	a.mu.RLock()
	entry := a.mu.jwksCache.jwks[srv.server.URL]
	a.mu.RUnlock()
	require.NotNil(t, entry.set)
	require.False(t, entry.freshness.fresh(timeutil.Now()))

	// Once the origin recovers, authentication succeeds again.
	srv.setJWKSStatus(0)
	srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 3)
}

func TestRemoteJWKSCacheMalformedResponses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("malformed discovery keeps the stale entry", func(t *testing.T) {
		srv := newTestOIDCServer()
		defer srv.Close()
		srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(0))
		key, set := newCacheTestKeys(t, "cache-kid-a")
		srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
		token := cacheTestToken(t, key, srv.server.URL)
		a, st := newCacheTestAuthenticator(t, srv.server.URL)

		require.NoError(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 1)

		srv.setDiscovery([]byte("not json"), nil)
		require.Error(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 2, 1)

		a.mu.RLock()
		entry, ok := a.mu.jwksCache.discovery[srv.server.URL]
		a.mu.RUnlock()
		require.True(t, ok)
		require.False(t, entry.freshness.fresh(timeutil.Now()))

		// The stale discovery document is not extended by the failure; the
		// next login refetches and succeeds once the origin recovers.
		srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(0))
		require.NoError(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 3, 1)
	})

	t.Run("malformed jwks keeps the stale set", func(t *testing.T) {
		srv := newTestOIDCServer()
		defer srv.Close()
		srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
		key, set := newCacheTestKeys(t, "cache-kid-a")
		srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(0))
		token := cacheTestToken(t, key, srv.server.URL)
		a, st := newCacheTestAuthenticator(t, srv.server.URL)

		require.NoError(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 1)

		srv.setJWKS(`{"keys": [`, nil)
		require.Error(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 2)

		a.mu.RLock()
		entry := a.mu.jwksCache.jwks[srv.server.URL]
		a.mu.RUnlock()
		require.NotNil(t, entry.set)
		require.False(t, entry.freshness.fresh(timeutil.Now()))

		srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
		require.NoError(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 3)
	})

	t.Run("empty jwks removes previously trusted keys", func(t *testing.T) {
		ctx := context.Background()
		srv := newTestOIDCServer()
		defer srv.Close()
		srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
		key, set := newCacheTestKeys(t, "cache-kid-a")
		srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(0))
		token := cacheTestToken(t, key, srv.server.URL)
		a, st := newCacheTestAuthenticator(t, srv.server.URL)

		require.NoError(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 1)

		srv.setJWKS(`{"keys": []}`, cacheMaxAgeHeader(300))
		require.Error(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 2)

		a.mu.RLock()
		entry := a.mu.jwksCache.jwks[srv.server.URL]
		a.mu.RUnlock()
		require.Equal(t, 0, entry.set.Len())

		// The empty set is trusted while fresh, so the withdrawn key stays
		// withdrawn. The unfamiliar key ID still buys one early refresh, which
		// fetches the empty set again; the cooldown then blocks further
		// attempts.
		require.Error(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 3)
		require.Error(t, cacheTestLogin(t, a, st, token))
		srv.assertHits(t, 1, 3)

		// A configuration change revokes the empty entry, and the recovered
		// origin can be used again.
		JWTAuthEnabled.Override(ctx, &st.SV, false)
		JWTAuthEnabled.Override(ctx, &st.SV, true)
		srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
		require.NoError(t, cacheTestLogin(t, a, st, token))
		// The full invalidation dropped the discovery document too.
		srv.assertHits(t, 2, 4)
	})
}

func TestRemoteJWKSCacheConfigInvalidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := newTestOIDCServer()
	defer srv.Close()
	srv.pointDiscoveryAtJWKS(cacheMaxAgeHeader(300))
	key, set := newCacheTestKeys(t, "cache-kid-a")
	srv.setJWKS(serializePublicKeySet(t, set), cacheMaxAgeHeader(300))
	token := cacheTestToken(t, key, srv.server.URL)
	a, st := newCacheTestAuthenticator(t, srv.server.URL)

	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 1)

	// An audience change does not affect key trust, and the ordinary config
	// reload that precedes every login preserves the cache.
	JWTAuthAudience.Override(ctx, &st.SV, "[\"extra\",\""+audience1+"\"]")
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 1)

	// Switching to an explicit issuer_jwks_map clears the cache, and skips
	// discovery.
	JWTAuthIssuersConfig.Override(ctx, &st.SV,
		fmt.Sprintf(`{"issuer_jwks_map": {%q: %q}}`, srv.server.URL, srv.server.URL+"/jwks"))
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 2)

	// Changing the custom CA clears the cache as well.
	caPEM, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedCACert))
	require.NoError(t, err)
	JWTAuthIssuerCustomCA.Override(ctx, &st.SV, string(caPEM))
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 3)

	// Disabling JWT authentication clears the cache; re-enabling starts cold.
	JWTAuthEnabled.Override(ctx, &st.SV, false)
	require.ErrorContains(t, cacheTestLogin(t, a, st, token), "not enabled")
	JWTAuthEnabled.Override(ctx, &st.SV, true)
	require.NoError(t, cacheTestLogin(t, a, st, token))
	srv.assertHits(t, 1, 4)
}

func TestRemoteJWKSCacheBodyLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("a"), maxRemoteDocumentBytes+1))
	}))
	defer srv.Close()

	a := &jwtAuthenticator{}
	a.mu.conf.httpClient = httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout)
	_, err := fetchRemoteDocument(context.Background(), srv.URL, a, nil)
	require.ErrorContains(t, err, "exceeds")
}

func TestRemoteJWKSCacheContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	a := &jwtAuthenticator{}
	a.mu.conf.httpClient = httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fetchRemoteDocument(ctx, srv.URL, a, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRemoteJWKSCacheStaticJWKSUnaffected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	JWTAuthEnabled.Override(ctx, &st.SV, true)
	JWTAuthIssuersConfig.Override(ctx, &st.SV, issuer1)
	JWTAuthAudience.Override(ctx, &st.SV, audience1)
	key, set := newCacheTestKeys(t, keyID1)
	token := cacheTestToken(t, key, issuer1)
	JWTAuthJWKS.Override(ctx, &st.SV, serializePublicKeySet(t, set))

	// Auto-fetch stays disabled, so any remote fetch is a test failure.
	restore := testutils.TestingHook(&fetchRemoteDocument,
		func(context.Context, string, *jwtAuthenticator, http.Header) (*remoteDocument, error) {
			return nil, errors.New("unexpected remote JWKS fetch")
		})
	defer restore()

	verifier := ConfigureJWTAuth(ctx, log.AmbientContext{}, st, uuid.Nil)
	_, err := verifier.ValidateJWTLogin(ctx, st,
		username.MakeSQLUsernameFromPreNormalizedString(username1), token, cacheTestIdentMap(t))
	require.NoError(t, err)
}

func TestIssuerURLConfEqual(t *testing.T) {
	conf := func(issuers []string, mappings map[string]string) issuerURLConf {
		c := issuerURLConf{issuers: issuers}
		if mappings != nil {
			c.ijMap = &issuerJWKSMap{Mappings: mappings}
		}
		return c
	}
	for _, tc := range []struct {
		name  string
		a, b  issuerURLConf
		equal bool
	}{
		{
			name:  "same list in different order",
			a:     conf([]string{"a", "b"}, nil),
			b:     conf([]string{"b", "a"}, nil),
			equal: true,
		},
		{
			name:  "different lists",
			a:     conf([]string{"a"}, nil),
			b:     conf([]string{"a", "b"}, nil),
			equal: false,
		},
		{
			name:  "same map",
			a:     conf([]string{"a"}, map[string]string{"a": "https://a"}),
			b:     conf([]string{"a"}, map[string]string{"a": "https://a"}),
			equal: true,
		},
		{
			name:  "changed mapping",
			a:     conf([]string{"a"}, map[string]string{"a": "https://a"}),
			b:     conf([]string{"a"}, map[string]string{"a": "https://b"}),
			equal: false,
		},
		{
			name:  "map versus list",
			a:     conf([]string{"a"}, map[string]string{"a": "https://a"}),
			b:     conf([]string{"a"}, nil),
			equal: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.equal, tc.a.equal(tc.b))
			require.Equal(t, tc.equal, tc.b.equal(tc.a))
		})
	}
}
