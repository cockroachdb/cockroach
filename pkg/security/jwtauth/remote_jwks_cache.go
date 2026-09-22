// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file implements the per-node cache of auto-fetched OpenID Connect
// discovery documents and JWK sets used by jwtAuthenticator.
//
// The cache follows the issuer's own cache contract (RFC 9111 freshness
// derived from Cache-Control and Expires) rather than inventing an
// operator-configured TTL, so it can never extend trust beyond what the issuer
// declared. It is fail-closed: an entry that is not fresh is never used
// without a successful refresh, and a failed refresh fails authentication
// instead of falling back to keys the issuer may have withdrawn. The one
// exception to origin freshness is the bounded early refresh for an unfamiliar
// key ID described on getJWKS.
//
// The cache holds no goroutines, persists nothing, and is not shared between
// nodes. All access happens under the owning authenticator's mutex.

package jwtauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
)

// maxRemoteDocumentBytes bounds how much of a discovery or JWKS response body
// is read into memory. Both documents are small, and the bound protects the
// node from a misbehaving issuer streaming an unbounded body.
const maxRemoteDocumentBytes = 1 << 20 // 1 MiB

// forcedJWKSRefreshCooldown bounds how often an unfamiliar key ID may bypass
// the freshness lifetime of a cached JWK set. The key ID is chosen by the
// token's sender, so without a bound any client could provoke one origin fetch
// per randomized key ID. The cooldown is deliberately short: it only needs to
// cover the time between an issuer starting to use a new signing key and the
// cache learning about it, while the ordinary freshness lifetime covers
// steady-state reuse.
const forcedJWKSRefreshCooldown = 30 * time.Second

// remoteDocument is a fetched discovery or JWKS response body together with
// the response metadata needed to decide whether it may be reused.
type remoteDocument struct {
	body      []byte
	freshness responseFreshness
}

// now returns the time used for cache freshness decisions. It defaults to the
// system clock; tests substitute a manual clock for deterministic expiry.
func (a *jwtAuthenticator) now() time.Time {
	if a.clock == nil {
		return timeutil.Now()
	}
	return a.clock()
}

// fetchRemoteDocument fetches url, returning the body along with the
// response's freshness metadata. The body is size-bounded and non-2xx
// responses are reported as errors rather than returned as payloads. Tests
// replace this function to avoid network access.
var fetchRemoteDocument = func(
	ctx context.Context, url string, a *jwtAuthenticator, header http.Header,
) (*remoteDocument, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = header.Clone()
	}
	// requestTime and responseTime bound how much of the response's age was
	// spent in transit (RFC 9111 §4.2.3).
	requestTime := a.now()
	resp, err := a.mu.conf.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseTime := a.now()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRemoteDocumentBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxRemoteDocumentBytes {
		return nil, errors.Newf(
			"response body exceeds %d bytes", maxRemoteDocumentBytes,
		)
	}
	if err := checkHTTPResponseStatus(url, resp, body); err != nil {
		return nil, err
	}
	return &remoteDocument{
		body:      body,
		freshness: parseResponseFreshness(resp.Header, requestTime, responseTime),
	}, nil
}

// responseFreshness records the RFC 9111 §4.2 inputs needed to decide whether
// a fetched response may be reused without contacting the origin. It is a
// response-freshness record, not a general-purpose HTTP cache entry: only
// explicit origin freshness is honored, and a response without usable
// Cache-Control/Expires metadata is treated as immediately stale. That keeps
// the pre-cache behavior of fetching on every use for issuers that do not
// publish cache metadata.
type responseFreshness struct {
	requestTime  time.Time
	responseTime time.Time
	date         time.Time     // zero when the response carried no usable Date
	age          time.Duration // parsed Age header; zero when absent or invalid
	lifetime     time.Duration // explicit freshness lifetime, when hasLifetime
	hasLifetime  bool
	noStore      bool // response must not be retained
	varyAll      bool // Vary: * — reuse is unpredictable and thus disallowed
}

// currentAge returns the response's age per RFC 9111 §4.2.3, using the
// conservative form that takes the larger of the apparent age and the
// corrected Age header value.
func (f responseFreshness) currentAge(now time.Time) time.Duration {
	var apparentAge time.Duration
	if !f.date.IsZero() {
		apparentAge = f.responseTime.Sub(f.date)
		if apparentAge < 0 {
			apparentAge = 0
		}
	}
	// The response was generated after requestTime and before responseTime;
	// attributing the whole delay to the response is the conservative choice.
	correctedAge := addDurationSat(f.age, f.responseTime.Sub(f.requestTime))
	if correctedAge < apparentAge {
		correctedAge = apparentAge
	}
	return addDurationSat(correctedAge, now.Sub(f.responseTime))
}

// addDurationSat returns a + b, saturating at the largest representable
// duration instead of wrapping around. Response ages sum an origin-provided
// date, an Age header, and local residence time; a wrap could turn an ancient
// response into a negative age and thus make it appear fresh forever.
func addDurationSat(a, b time.Duration) time.Duration {
	if b > 0 && a > time.Duration(math.MaxInt64)-b {
		return time.Duration(math.MaxInt64)
	}
	if b < 0 && a < time.Duration(math.MinInt64)-b {
		return time.Duration(math.MinInt64)
	}
	return a + b
}

// fresh reports whether the response may be reused now without contacting the
// origin, per RFC 9111 §4.2. Responses that carry no usable freshness
// metadata, forbid storage, or vary unpredictably are never reusable.
func (f responseFreshness) fresh(now time.Time) bool {
	if !f.hasLifetime || f.noStore || f.varyAll {
		return false
	}
	return f.lifetime > f.currentAge(now)
}

// parseResponseFreshness extracts freshness metadata from the response headers
// of a request that was sent at requestTime and completed at responseTime.
//
// Directives that force revalidation are folded into "no usable lifetime":
// this cache never serves stale responses, so revalidating before every reuse
// (no-cache) and refetching on every use are the same behavior here.
// must-revalidate is honored implicitly by the same fail-closed policy.
func parseResponseFreshness(h http.Header, requestTime, responseTime time.Time) responseFreshness {
	f := responseFreshness{requestTime: requestTime, responseTime: responseTime}
	if date, err := http.ParseTime(h.Get("Date")); err == nil {
		f.date = date
	}
	if age, ok := parseDeltaSeconds(h.Get("Age")); ok {
		f.age = age
	}
	for _, vary := range h.Values("Vary") {
		if strings.Contains(vary, "*") {
			f.varyAll = true
		}
	}

	// maxAgeSeen tracks presence, not validity: per RFC 9111 §4.2.1 a max-age
	// directive overrides Expires even when the directive itself is unusable,
	// in which case the response must be treated as stale.
	var (
		maxAgeSeen  bool
		maxAgeValid = true
		maxAge      time.Duration
		noReuse     bool
	)
	// Multiple Cache-Control header lines are equivalent to one comma-separated
	// list (RFC 9110 §5.3), and a directive such as no-store on any line must
	// be honored. A value with unterminated quoting is unusable freshness.
	cacheControl, ok := splitCacheControl(strings.Join(h.Values("Cache-Control"), ","))
	if !ok {
		// The value cannot be parsed reliably, so it may have declared the
		// response non-storable; do not retain it.
		f.noStore = true
		return f
	}
	for _, directive := range cacheControl {
		name, value, _ := strings.Cut(directive, "=")
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "no-store":
			f.noStore = true
		case "no-cache":
			noReuse = true
		case "max-age":
			if maxAgeSeen {
				// Duplicate or conflicting freshness information is treated
				// as stale (RFC 9111 §4.2.1).
				maxAgeValid = false
				continue
			}
			maxAgeSeen = true
			secs, ok := parseDeltaSeconds(value)
			if !ok {
				maxAgeValid = false
				continue
			}
			maxAge = secs
		case "must-revalidate", "s-maxage":
			// must-revalidate: implied by the fail-closed policy above.
			// s-maxage: applies to shared caches only; this is a private one.
		}
	}
	if noReuse || f.noStore || f.varyAll {
		return f
	}
	if maxAgeSeen {
		if maxAgeValid {
			f.lifetime, f.hasLifetime = maxAge, true
		}
		return f
	}
	// Fall back to Expires - Date, using the time the response was received
	// when Date is absent. An unparsable Expires (including the value "0") is
	// already expired per RFC 9111 §5.3, and conflicting Expires values are
	// treated as stale rather than letting the first one win.
	expiresValues := h.Values("Expires")
	if len(expiresValues) != 1 {
		return f
	}
	expires, err := http.ParseTime(expiresValues[0])
	if err != nil {
		return f
	}
	base := f.date
	if base.IsZero() {
		base = responseTime
	}
	f.lifetime, f.hasLifetime = expires.Sub(base), true
	return f
}

// splitCacheControl splits a Cache-Control field value on commas that are not
// inside a quoted string. Quoted-string values may legally contain commas, and
// treating them as separators could manufacture a directive out of extension
// data. Backslash escapes inside quoted strings are respected. It reports
// false when a quoted string is left unterminated, in which case no directive
// can be trusted.
func splitCacheControl(value string) ([]string, bool) {
	var (
		directives []string
		start      int
		quoted     bool
		escaped    bool
	)
	for i := 0; i < len(value); i++ {
		switch c := value[i]; {
		case escaped:
			escaped = false
		case quoted && c == '\\':
			escaped = true
		case c == '"':
			quoted = !quoted
		case c == ',' && !quoted:
			directives = append(directives, value[start:i])
			start = i + 1
		}
	}
	if quoted {
		return nil, false
	}
	return append(directives, value[start:]), true
}

// parseDeltaSeconds parses an RFC 9111 §1.2.2 delta-seconds value. Values that
// overflow are clamped to 2^31 seconds as the RFC requires. It reports false
// when s is not a non-negative integer.
func parseDeltaSeconds(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			// delta-seconds is a non-negative integer, so anything else
			// (including a negative or malformed value) is unusable freshness.
			return 0, false
		}
	}
	// Every byte is a digit, so an overflow is a large non-negative value;
	// clamp it per RFC 9111 §1.2.2.
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Duration(math.MaxInt32) * time.Second, true
	}
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	return time.Duration(n) * time.Second, true
}

// discoveryEntry caches the validated result of one issuer's OpenID
// configuration document.
type discoveryEntry struct {
	jwksURI   string
	freshness responseFreshness
}

// jwksEntry caches a parsed JWK set for one issuer.
//
// The cached set is tied to the exact source URL it was fetched from: when the
// issuer's discovery document starts advertising a different jwks_uri, the
// entry is abandoned rather than reused.
type jwksEntry struct {
	sourceURL string
	set       jwk.Set
	freshness responseFreshness
}

// remoteJWKSCache is the per-node cache of auto-fetched discovery documents
// and JWK sets. It must be guarded by the owning authenticator's mutex.
//
// Entries are keyed by configured issuer, so the number of entries is bounded
// by the issuer list rather than by request traffic. The cache holds no
// goroutines and performs no background refresh; expiry is evaluated on the
// authentication path.
type remoteJWKSCache struct {
	discovery map[string]discoveryEntry
	jwks      map[string]jwksEntry
	// forcedRefresh records the most recent freshness-bypassing refresh
	// attempt per issuer. It is deliberately independent of the cached
	// documents: replacing or dropping a document must not reset the
	// unfamiliar-kid cooldown.
	forcedRefresh map[string]time.Time
}

// invalidateJWKSCache drops every cached discovery document and JWK set. It is
// called when a configuration change revokes the trust that cached entries
// were fetched under. The cache is left usable, with empty maps, for the next
// authentication.
func (a *jwtAuthenticator) invalidateJWKSCache() {
	a.mu.jwksCache = newRemoteJWKSCache()
}

// newRemoteJWKSCache returns an empty cache with initialized maps so that
// cache writers need no lazy-initialization guards.
func newRemoteJWKSCache() remoteJWKSCache {
	return remoteJWKSCache{
		discovery:     make(map[string]discoveryEntry),
		jwks:          make(map[string]jwksEntry),
		forcedRefresh: make(map[string]time.Time),
	}
}

// cacheDiscoveryEntry stores the discovery result for issuer unless the
// response must not be retained.
func (a *jwtAuthenticator) cacheDiscoveryEntry(
	issuer, jwksURI string, freshness responseFreshness,
) {
	if freshness.noStore {
		delete(a.mu.jwksCache.discovery, issuer)
		return
	}
	a.mu.jwksCache.discovery[issuer] = discoveryEntry{
		jwksURI:   jwksURI,
		freshness: freshness,
	}
}

// resolveJWKSURI returns the JWKS URI to use for issuer, served from the
// cached discovery document while that document is fresh.
func (a *jwtAuthenticator) resolveJWKSURI(ctx context.Context, issuer string) (string, error) {
	// An explicit issuer_jwks_map skips discovery entirely, exactly as it did
	// before the cache was introduced.
	if err := a.mu.conf.issuersConf.checkJWKSConfigured(); err == nil {
		return a.mu.conf.issuersConf.getJWKSURI(issuer)
	}
	now := a.now()
	if entry, ok := a.mu.jwksCache.discovery[issuer]; ok && entry.freshness.fresh(now) {
		return entry.jwksURI, nil
	}
	resp, err := fetchRemoteDocument(ctx, getOpenIdConfigEndpoint(issuer), a, nil)
	if err != nil {
		return "", err
	}
	var config struct {
		JWKSUri string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(resp.body, &config); err != nil {
		return "", err
	}
	if config.JWKSUri == "" {
		return "", errors.Newf("no JWKS URI found in OpenID configuration")
	}
	// Validate the advertised URI before caching it: an unusable value would
	// otherwise be reused for its whole freshness lifetime and turn a
	// transient malformed response into a lasting authentication outage.
	jwksURL, err := url.Parse(config.JWKSUri)
	if err != nil || !jwksURL.IsAbs() || jwksURL.Host == "" ||
		(jwksURL.Scheme != "http" && jwksURL.Scheme != "https") {
		return "", errors.Newf("invalid JWKS URI in OpenID configuration")
	}
	a.cacheDiscoveryEntry(issuer, config.JWKSUri, resp.freshness)
	return config.JWKSUri, nil
}

// getJWKS returns a JWK set for verifying a token issued by issuer whose JOSE
// header carries the given key ID.
//
// A fresh cached set is returned without contacting the origin. An unfamiliar
// key ID may indicate that the issuer rotated its signing keys (OIDC Core
// §10.1.1), so it triggers one early refresh that bypasses freshness, subject
// to a per-issuer cooldown. Key IDs are attacker-controlled, so at most one
// fetch per cooldown can be provoked no matter how many unfamiliar key IDs are
// presented. When no usable set can be obtained, the error is returned and
// authentication fails closed.
func (a *jwtAuthenticator) getJWKS(ctx context.Context, issuer, kid string) (jwk.Set, error) {
	jwksURI, err := a.resolveJWKSURI(ctx, issuer)
	if err != nil {
		return nil, err
	}
	now := a.now()
	entry, ok := a.mu.jwksCache.jwks[issuer]
	if ok && entry.sourceURL != jwksURI {
		// The issuer no longer advertises the source this entry was fetched
		// from. Drop it so the old source's keys can never be resurrected if
		// discovery later flips back.
		delete(a.mu.jwksCache.jwks, issuer)
		ok = false
	}
	if ok && entry.freshness.fresh(now) {
		if kid == "" {
			return entry.set, nil
		}
		if _, ok := entry.set.LookupKeyID(kid); ok {
			return entry.set, nil
		}
		if now.Sub(a.mu.jwksCache.forcedRefresh[issuer]) < forcedJWKSRefreshCooldown {
			// The cooldown has not elapsed. Verification still runs against
			// the cached set and fails if the key is genuinely absent.
			return entry.set, nil
		}
		return a.refreshJWKS(ctx, issuer, jwksURI, true /* forceRevalidate */, now)
	}
	// The entry is missing or stale; fetch a replacement.
	return a.refreshJWKS(ctx, issuer, jwksURI, false, now)
}

// refreshJWKS fetches the JWK set serving sourceURL and, unless the response
// must not be retained, replaces issuer's cache entry with it. The entry is
// replaced only after the body parses as a JWK set, so a malformed or failed
// refresh never revives old keys or extends their freshness.
//
// forceRevalidate adds a Cache-Control: no-cache request directive (so
// intermediaries revalidate rather than serve their own copy) and records the
// attempt in the issuer's cooldown, which survives document replacement, so
// that repeated unfamiliar key IDs cannot each provoke a fetch. The caller
// must hold the authenticator's mutex.
func (a *jwtAuthenticator) refreshJWKS(
	ctx context.Context, issuer, sourceURL string, forceRevalidate bool, now time.Time,
) (jwk.Set, error) {
	var header http.Header
	if forceRevalidate {
		header = http.Header{"Cache-Control": []string{"no-cache"}}
		// Record the attempt before fetching, so the cooldown covers it
		// regardless of outcome. This is the only place the cooldown is armed.
		a.mu.jwksCache.forcedRefresh[issuer] = now
	}
	resp, err := fetchRemoteDocument(ctx, sourceURL, a, header)
	if err != nil {
		return nil, err
	}
	set, err := jwk.Parse(resp.body)
	if err != nil {
		return nil, err
	}
	if err := validateJWKSet(set); err != nil {
		return nil, err
	}
	if resp.freshness.noStore {
		delete(a.mu.jwksCache.jwks, issuer)
	} else {
		a.mu.jwksCache.jwks[issuer] = jwksEntry{
			sourceURL: sourceURL,
			set:       set,
			freshness: resp.freshness,
		}
	}
	return set, nil
}

// validateJWKSet rejects a JWK set that contains malformed or unusable key
// material. jwk.Parse only checks the document's shape, so a key may parse yet
// be unable to verify anything; caching such a set with a known key ID would
// block recovery for its whole freshness lifetime. An empty set is valid and
// is what removes previously trusted keys.
func validateJWKSet(set jwk.Set) error {
	for i := 0; i < set.Len(); i++ {
		key, ok := set.Key(i)
		if !ok {
			continue
		}
		if err := validateJWKKey(key); err != nil {
			return err
		}
	}
	return nil
}

// validateJWKKey rejects a key whose public components cannot verify
// signatures. jwk validation only checks that required members exist and have
// plausible shapes: a zero RSA modulus or an EC point that is not on its curve
// still passes it, and caching such a set under a known key ID would block
// recovery for the set's whole freshness lifetime. Other key types are
// accepted on jwk validation alone.
func validateJWKKey(key jwk.Key) error {
	if err := key.Validate(); err != nil {
		return errors.Wrap(err, "invalid JWK in JWK set")
	}
	if key.KeyType() == jwa.OKP {
		return validateOKPKey(key)
	}
	// Materialize the raw key first: a JWK may carry private-key material, in
	// which case a direct conversion to a public-key type would fail even
	// though its public half is usable.
	var raw any
	if err := key.Raw(&raw); err != nil {
		return errors.Wrap(err, "invalid JWK in JWK set")
	}
	switch k := raw.(type) {
	case *rsa.PublicKey:
		return validateRSAPublicKey(k)
	case *rsa.PrivateKey:
		return validateRSAPublicKey(&k.PublicKey)
	case *ecdsa.PublicKey:
		return validateECPublicKey(k)
	case *ecdsa.PrivateKey:
		return validateECPublicKey(&k.PublicKey)
	}
	return nil
}

// okpCoordinateSizes lists the RFC 8037 public coordinate sizes for the OKP
// curves this cache can check.
var okpCoordinateSizes = map[jwa.EllipticCurveAlgorithm]int{
	jwa.Ed25519: ed25519.PublicKeySize,
	jwa.X25519:  32,
	jwa.Ed448:   57,
	jwa.X448:    56,
}

// validateOKPKey checks the decoded public coordinate of an OKP key, which the
// library materializes without length validation, and additionally requires
// materialization for the curves it supports so that inconsistent private
// material is rejected. Curves outside the size table are left to structural
// validation.
func validateOKPKey(key jwk.Key) error {
	crv, ok := key.Get(jwk.OKPCrvKey)
	if !ok {
		return nil
	}
	curve, ok := crv.(jwa.EllipticCurveAlgorithm)
	if !ok {
		return nil
	}
	if want, known := okpCoordinateSizes[curve]; known {
		x, ok := key.Get(jwk.OKPXKey)
		if !ok {
			return errors.New("unusable OKP public key in JWK set")
		}
		coord, ok := x.([]byte)
		if !ok || len(coord) != want {
			return errors.New("unusable OKP public key in JWK set")
		}
		switch curve {
		case jwa.Ed25519, jwa.X25519:
			// The library can materialize these curves, so a failure here
			// means the remaining key material is inconsistent.
			var raw any
			if err := key.Raw(&raw); err != nil {
				return errors.Wrap(err, "invalid JWK in JWK set")
			}
		}
	}
	return nil
}

func validateRSAPublicKey(pub *rsa.PublicKey) error {
	if pub.N == nil || pub.N.Sign() <= 0 || pub.E < 2 || pub.E > 1<<31-1 {
		return errors.New("unusable RSA public key in JWK set")
	}
	return nil
}

func validateECPublicKey(pub *ecdsa.PublicKey) error {
	if pub.Curve == nil || pub.X == nil || pub.Y == nil || !pub.Curve.IsOnCurve(pub.X, pub.Y) {
		return errors.New("unusable EC public key in JWK set")
	}
	return nil
}

// tokenKeyID extracts the key ID from a token's protected JOSE header, or ""
// when the header does not carry one. The value is untrusted and is only ever
// used as a lookup hint against configured issuers.
func tokenKeyID(tokenBytes []byte) string {
	msg, err := jws.Parse(tokenBytes)
	if err != nil {
		// Callers reject unparsable tokens before this hint matters. Treating
		// the key ID as absent also keeps a malformed token from triggering an
		// early refresh.
		return ""
	}
	sigs := msg.Signatures()
	if len(sigs) == 0 {
		return ""
	}
	headers := sigs[0].ProtectedHeaders()
	if headers == nil {
		return ""
	}
	return headers.KeyID()
}
