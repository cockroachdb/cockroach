// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauth

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
)

const (
	// jwksCacheMinInterval bounds how often the background refresher may refetch a
	// JWKS: even if the identity provider advertises a very short freshness
	// lifetime, we never refetch more frequently than this. It protects the IdP
	// from excessive traffic.
	jwksCacheMinInterval = 30 * time.Second
	// jwksCacheMaxInterval bounds the interval between refresh attempts when the
	// identity provider advertises a long (or no) freshness lifetime. A failed
	// refresh retains the last successfully fetched set, so this does not impose a
	// maximum stale age during an IdP outage.
	jwksCacheMaxInterval = 5 * time.Minute
	// unknownKIDRefreshCooldown rate-limits forced refreshes triggered by a token
	// whose key ID is absent from the cached set, so a burst of tokens carrying an
	// unrecognized kid cannot stampede the identity provider.
	unknownKIDRefreshCooldown = 30 * time.Second
	// discoveryTTL bounds how long a jwks_uri resolved from an issuer's OpenID
	// Connect discovery document is reused before it is re-resolved.
	discoveryTTL = 5 * time.Minute
)

// discoveryEntry is a cached OIDC-discovery result: the jwks_uri resolved for an
// issuer and the time after which it must be re-resolved.
type discoveryEntry struct {
	jwksURI string
	expiry  time.Time
}

// JWKSCache caches auto-fetched JWK sets per issuer for a single node, so that
// steady-state JWT/OAUTHBEARER logins do not touch the network. It wraps jwx
// v3's jwk.Cache, which refreshes each registered JWKS in the background
// according to the issuer's own HTTP cache headers (RFC 9111), bounded by
// [jwksCacheMinInterval, jwksCacheMaxInterval]. It additionally caches the OIDC
// discovery step (issuer -> jwks_uri) and forces an early, rate-limited refresh
// when a token presents a key ID that is not in the cached set (key rotation).
//
// Lifecycle: created once per authenticator via NewJWKSCache with a
// server-lifetime context; the background refresh goroutine stops when that
// context is cancelled. When a configuration change affects fetching (issuer
// config, custom CA, client timeout), the owner builds and installs a new cache
// before discarding this one. If construction fails, the previous cache remains
// active under its existing fetch configuration.
//
// The cache is safe for concurrent use.
type JWKSCache struct {
	// ctx is the cache-lifetime context that owns the background refresh
	// goroutine. It is a descendant of the server context, cancelled by Close (so
	// a replaced cache stops refreshing). Per-request work uses the caller's
	// context.
	ctx    context.Context
	cancel context.CancelFunc
	cache  *jwk.Cache
	// httpClient is the stable HTTP client (custom CA + timeout) used for both
	// discovery and JWKS fetches. It is fixed for the life of the cache; a CA or
	// timeout change is handled by the owner replacing the whole cache.
	httpClient *httputil.Client

	mu struct {
		syncutil.Mutex
		registered map[string]struct{}       // jwks_uri -> already registered
		discovery  map[string]discoveryEntry // issuer -> resolved jwks_uri
		lastForced map[string]time.Time      // jwks_uri -> last forced refresh
	}
}

// NewJWKSCache constructs a JWKSCache. serverCtx must live for as long as the
// authenticator; the cache derives its own cancelable context from it so that
// Close stops the background refresh goroutine (used when the cache is replaced
// after a configuration change). httpClient is used for all discovery and JWKS
// fetches.
func NewJWKSCache(serverCtx context.Context, httpClient *httputil.Client) (*JWKSCache, error) {
	ctx, cancel := context.WithCancel(serverCtx)
	cache, err := jwk.NewCache(ctx, httprc.NewClient(httprc.WithHTTPClient(httpClient)))
	if err != nil {
		cancel()
		return nil, err
	}
	c := &JWKSCache{ctx: ctx, cancel: cancel, cache: cache, httpClient: httpClient}
	c.mu.registered = make(map[string]struct{})
	c.mu.discovery = make(map[string]discoveryEntry)
	c.mu.lastForced = make(map[string]time.Time)
	return c, nil
}

// Close stops the cache's background refresh goroutine. It is called when the
// cache is replaced (configuration change) or the authenticator is torn down.
func (c *JWKSCache) Close() {
	c.cancel()
}

// KeySet returns the JWK set to verify a token from issuer, served from the
// per-node cache. kid is the token's key ID (may be empty); if it is non-empty
// and absent from the cached set, KeySet forces one rate-limited refresh to pick
// up a freshly rotated key before returning.
func (c *JWKSCache) KeySet(
	ctx context.Context, issuersConf *issuerURLConf, issuer, kid string,
) (jwk.Set, error) {
	// Tie cache operations to both the login and cache lifetimes. This is
	// necessary because a configuration change may close this cache after a
	// caller loaded it but before the caller enters the underlying controller.
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	// AfterFunc runs asynchronously, so synchronously propagate an
	// already-canceled cache context before entering the controller.
	if c.ctx.Err() != nil {
		cancel()
	}

	jwksURI, err := c.resolveJWKSURI(ctx, issuersConf, issuer)
	if err != nil {
		return nil, err
	}
	if err := c.ensureRegistered(ctx, jwksURI); err != nil {
		return nil, err
	}
	set, err := c.cache.Lookup(ctx, jwksURI)
	if err != nil {
		return nil, err
	}
	if kid != "" {
		if _, ok := set.LookupKeyID(kid); !ok {
			if refreshed, ok := c.maybeForceRefresh(ctx, jwksURI); ok {
				set = refreshed
			}
		}
	}
	return set, nil
}

// resolveJWKSURI returns the jwks_uri for issuer: the explicit URI from the
// issuer config when configured, otherwise the result of OIDC discovery, cached
// per issuer for discoveryTTL.
func (c *JWKSCache) resolveJWKSURI(
	ctx context.Context, issuersConf *issuerURLConf, issuer string,
) (string, error) {
	if issuersConf.HasExplicitJWKSMap() {
		return issuersConf.GetJWKSURI(issuer)
	}
	if entry, ok := c.lookupDiscovery(issuer); ok && timeutil.Now().Before(entry.expiry) {
		return entry.jwksURI, nil
	}
	// Resolve via discovery; this performs a network call and holds no lock.
	uri, err := getJWKSURIFromWellKnown(ctx, c.httpClient, issuer)
	if err != nil {
		return "", err
	}
	c.storeDiscovery(issuer, uri)
	return uri, nil
}

// ensureRegistered registers jwksURI with the background-refreshing cache on
// first use. Registration performs the initial (synchronous) fetch, so the set
// is populated before the first login for a new issuer completes.
func (c *JWKSCache) ensureRegistered(ctx context.Context, jwksURI string) error {
	if c.alreadyRegistered(jwksURI) {
		return nil
	}
	// Register without waiting so concurrent callers can all wait on the one
	// registered resource under their own contexts. Presence in the controller is
	// not readiness: only mark the URI locally after its first successful fetch.
	if err := c.cache.Register(ctx, jwksURI,
		jwk.WithMinInterval(jwksCacheMinInterval),
		jwk.WithMaxInterval(jwksCacheMaxInterval),
		jwk.WithHTTPClient(c.httpClient),
		jwk.WithWaitReady(false),
	); err != nil && !errors.Is(err, httprc.ErrResourceAlreadyExists()) {
		return err
	}
	resource, err := c.cache.LookupResource(ctx, jwksURI)
	if err != nil {
		return err
	}
	// Bound the wait for the initial background fetch by the client timeout so a
	// login against an unresponsive IdP fails cleanly rather than blocking on a
	// cold cache. A zero timeout imposes no bound, matching the HTTP client.
	ready := func(ctx context.Context) error { return resource.Ready(ctx) }
	if timeout := c.httpClient.Timeout; timeout > 0 {
		err = timeutil.RunWithTimeout(ctx, "waiting for JWKS fetch", timeout, ready)
	} else {
		err = ready(ctx)
	}
	if err != nil {
		return errors.Wrapf(err, "waiting for JWKS %q", jwksURI)
	}
	c.markRegistered(jwksURI)
	return nil
}

// maybeForceRefresh forces a refresh of jwksURI unless one was already forced
// within unknownKIDRefreshCooldown, so a flood of tokens with an unrecognized
// kid cannot stampede the identity provider. It returns the refreshed set when
// successful; callers otherwise continue with the previous set.
func (c *JWKSCache) maybeForceRefresh(ctx context.Context, jwksURI string) (jwk.Set, bool) {
	if !c.claimForcedRefresh(jwksURI) {
		return nil, false
	}
	set, err := c.cache.Refresh(ctx, jwksURI)
	if err != nil {
		// claimForcedRefresh rate-limits this warning along with the refresh.
		log.Ops.Warningf(ctx, "failed to refresh JWKS from %s: %v", jwksURI, err)
		return nil, false
	}
	return set, true
}

// The following small helpers isolate each critical section behind the mutex so
// that no network call is made while the lock is held.

func (c *JWKSCache) lookupDiscovery(issuer string) (discoveryEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.mu.discovery[issuer]
	return e, ok
}

func (c *JWKSCache) storeDiscovery(issuer, jwksURI string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.discovery[issuer] = discoveryEntry{jwksURI: jwksURI, expiry: timeutil.Now().Add(discoveryTTL)}
}

func (c *JWKSCache) alreadyRegistered(jwksURI string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.mu.registered[jwksURI]
	return ok
}

func (c *JWKSCache) markRegistered(jwksURI string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.registered[jwksURI] = struct{}{}
}

// claimForcedRefresh reports whether a forced refresh of jwksURI is permitted
// now (i.e. the cooldown has elapsed), recording the attempt time when true.
func (c *JWKSCache) claimForcedRefresh(jwksURI string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	last := c.mu.lastForced[jwksURI]
	if !last.IsZero() && timeutil.Since(last) < unknownKIDRefreshCooldown {
		return false
	}
	c.mu.lastForced[jwksURI] = timeutil.Now()
	return true
}

// keyIDFromToken returns the key ID from a JWS-signed token's protected header,
// or "" if the token is unparseable or carries no kid. It never returns an
// error: an unverified header read is best-effort input to the cache's
// unknown-kid handling, and a malformed token is rejected later by verification.
func keyIDFromToken(tokenBytes []byte) string {
	msg, err := jws.Parse(tokenBytes)
	if err != nil {
		return ""
	}
	sigs := msg.Signatures()
	if len(sigs) == 0 {
		return ""
	}
	kid, _ := sigs[0].ProtectedHeaders().KeyID()
	return kid
}
