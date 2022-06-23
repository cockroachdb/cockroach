package auth

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
)

type digestClient struct {
	nc       uint64
	lastSeen int64
}

// DigestAuth is an authenticator implementation for 'Digest' HTTP Authentication scheme (RFC 7616).
//
// Note: this implementation was written following now deprecated RFC
// 2617, and supports only MD5 algorithm.
//
// TODO: Add support for SHA-256 and SHA-512/256 algorithms.
type DigestAuth struct {
	Realm            string
	Opaque           string
	Secrets          SecretProvider
	PlainTextSecrets bool
	IgnoreNonceCount bool
	// Headers used by authenticator. Set to ProxyHeaders to use with
	// proxy server. When nil, NormalHeaders are used.
	Headers *Headers

	/*
	   Approximate size of Client's Cache. When actual number of
	   tracked client nonces exceeds
	   ClientCacheSize+ClientCacheTolerance, ClientCacheTolerance*2
	   older entries are purged.
	*/
	ClientCacheSize      int
	ClientCacheTolerance int

	clients map[string]*digestClient
	mutex   sync.RWMutex
}

// check that DigestAuth implements AuthenticatorInterface
var _ = (AuthenticatorInterface)((*DigestAuth)(nil))

type digestCacheEntry struct {
	nonce    string
	lastSeen int64
}

type digestCache []digestCacheEntry

func (c digestCache) Less(i, j int) bool {
	return c[i].lastSeen < c[j].lastSeen
}

func (c digestCache) Len() int {
	return len(c)
}

func (c digestCache) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Purge removes count oldest entries from DigestAuth.clients
func (da *DigestAuth) Purge(count int) {
	da.mutex.Lock()
	defer da.mutex.Unlock()
	entries := make([]digestCacheEntry, 0, len(da.clients))
	for nonce, client := range da.clients {
		entries = append(entries, digestCacheEntry{nonce, client.lastSeen})
	}
	cache := digestCache(entries)
	sort.Sort(cache)
	for _, client := range cache[:count] {
		delete(da.clients, client.nonce)
	}
}

// RequireAuth is an http.HandlerFunc which initiates the
// authentication process (or requires reauthentication).
func (da *DigestAuth) RequireAuth(w http.ResponseWriter, r *http.Request) {
	da.mutex.RLock()
	clientsLen := len(da.clients)
	da.mutex.RUnlock()

	if clientsLen > da.ClientCacheSize+da.ClientCacheTolerance {
		da.Purge(da.ClientCacheTolerance * 2)
	}
	nonce := RandomKey()

	da.mutex.Lock()
	da.clients[nonce] = &digestClient{nc: 0, lastSeen: time.Now().UnixNano()}
	da.mutex.Unlock()

	da.mutex.RLock()
	w.Header().Set(contentType, da.Headers.V().UnauthContentType)
	w.Header().Set(da.Headers.V().Authenticate,
		fmt.Sprintf(`Digest realm="%s", nonce="%s", opaque="%s", algorithm=MD5, qop="auth"`,
			da.Realm, nonce, da.Opaque))
	w.WriteHeader(da.Headers.V().UnauthCode)
	w.Write([]byte(da.Headers.V().UnauthResponse))
	da.mutex.RUnlock()
}

// DigestAuthParams parses Authorization header from the
// http.Request. Returns a map of auth parameters or nil if the header
// is not a valid parsable Digest auth header.
func DigestAuthParams(authorization string) map[string]string {
	s := strings.SplitN(authorization, " ", 2)
	if len(s) != 2 || s[0] != "Digest" {
		return nil
	}

	return ParsePairs(s[1])
}

// CheckAuth checks whether the request contains valid authentication
// data. Returns a pair of username, authinfo, where username is the
// name of the authenticated user or an empty string and authinfo is
// the contents for the optional Authentication-Info response header.
func (da *DigestAuth) CheckAuth(r *http.Request) (username string, authinfo *string) {
	da.mutex.RLock()
	defer da.mutex.RUnlock()
	username = ""
	authinfo = nil
	auth := DigestAuthParams(r.Header.Get(da.Headers.V().Authorization))
	if auth == nil {
		return "", nil
	}
	// RFC2617 Section 3.2.1 specifies that unset value of algorithm in
	// WWW-Authenticate Response header should be treated as
	// "MD5". According to section 3.2.2 the "algorithm" value in
	// subsequent Request Authorization header must be set to whatever
	// was supplied in the WWW-Authenticate Response header. This
	// implementation always returns an algorithm in WWW-Authenticate
	// header, however there seems to be broken clients in the wild
	// which do not set the algorithm. Assume the unset algorithm in
	// Authorization header to be equal to MD5.
	if _, ok := auth["algorithm"]; !ok {
		auth["algorithm"] = "MD5"
	}
	if da.Opaque != auth["opaque"] || auth["algorithm"] != "MD5" || auth["qop"] != "auth" {
		return "", nil
	}

	// Check if the requested URI matches auth header
	if r.RequestURI != auth["uri"] {
		// We allow auth["uri"] to be a full path prefix of request-uri
		// for some reason lost in history, which is probably wrong, but
		// used to be like that for quite some time
		// (https://tools.ietf.org/html/rfc2617#section-3.2.2 explicitly
		// says that auth["uri"] is the request-uri).
		//
		// TODO: make an option to allow only strict checking.
		switch u, err := url.Parse(auth["uri"]); {
		case err != nil:
			return "", nil
		case r.URL == nil:
			return "", nil
		case len(u.Path) > len(r.URL.Path):
			return "", nil
		case !strings.HasPrefix(r.URL.Path, u.Path):
			return "", nil
		}
	}

	HA1 := da.Secrets(auth["username"], da.Realm)
	if da.PlainTextSecrets {
		HA1 = H(auth["username"] + ":" + da.Realm + ":" + HA1)
	}
	HA2 := H(r.Method + ":" + auth["uri"])
	KD := H(strings.Join([]string{HA1, auth["nonce"], auth["nc"], auth["cnonce"], auth["qop"], HA2}, ":"))

	if subtle.ConstantTimeCompare([]byte(KD), []byte(auth["response"])) != 1 {
		return "", nil
	}

	// At this point crypto checks are completed and validated.
	// Now check if the session is valid.

	nc, err := strconv.ParseUint(auth["nc"], 16, 64)
	if err != nil {
		return "", nil
	}

	client, ok := da.clients[auth["nonce"]]
	if !ok {
		return "", nil
	}
	if client.nc != 0 && client.nc >= nc && !da.IgnoreNonceCount {
		return "", nil
	}
	client.nc = nc
	client.lastSeen = time.Now().UnixNano()

	respHA2 := H(":" + auth["uri"])
	rspauth := H(strings.Join([]string{HA1, auth["nonce"], auth["nc"], auth["cnonce"], auth["qop"], respHA2}, ":"))

	info := fmt.Sprintf(`qop="auth", rspauth="%s", cnonce="%s", nc="%s"`, rspauth, auth["cnonce"], auth["nc"])
	return auth["username"], &info
}

// Default values for ClientCacheSize and ClientCacheTolerance for DigestAuth
const (
	DefaultClientCacheSize      = 1000
	DefaultClientCacheTolerance = 100
)

// Wrap returns an http.HandlerFunc wraps AuthenticatedHandlerFunc
// with this DigestAuth authentication checks. Once the request
// contains valid credentials, it calls wrapped
// AuthenticatedHandlerFunc.
//
// Deprecated: new code should use NewContext instead.
func (da *DigestAuth) Wrap(wrapped AuthenticatedHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if username, authinfo := da.CheckAuth(r); username == "" {
			da.RequireAuth(w, r)
		} else {
			ar := &AuthenticatedRequest{Request: *r, Username: username}
			if authinfo != nil {
				w.Header().Set(da.Headers.V().AuthInfo, *authinfo)
			}
			wrapped(w, ar)
		}
	}
}

// JustCheck returns a new http.HandlerFunc, which requires
// DigestAuth to successfully authenticate a user before calling
// wrapped http.HandlerFunc.
//
// Authenticated Username is passed as an extra
// X-Authenticated-Username header to the wrapped HandlerFunc.
func (da *DigestAuth) JustCheck(wrapped http.HandlerFunc) http.HandlerFunc {
	return da.Wrap(func(w http.ResponseWriter, ar *AuthenticatedRequest) {
		ar.Header.Set(AuthUsernameHeader, ar.Username)
		wrapped(w, &ar.Request)
	})
}

// NewContext returns a context carrying authentication information for the request.
func (da *DigestAuth) NewContext(ctx context.Context, r *http.Request) context.Context {
	username, authinfo := da.CheckAuth(r)
	da.mutex.Lock()
	defer da.mutex.Unlock()
	info := &Info{Username: username, ResponseHeaders: make(http.Header)}
	if username != "" {
		info.Authenticated = true
		info.ResponseHeaders.Set(da.Headers.V().AuthInfo, *authinfo)
	} else {
		// return back digest WWW-Authenticate header
		if len(da.clients) > da.ClientCacheSize+da.ClientCacheTolerance {
			da.Purge(da.ClientCacheTolerance * 2)
		}
		nonce := RandomKey()
		da.clients[nonce] = &digestClient{nc: 0, lastSeen: time.Now().UnixNano()}
		info.ResponseHeaders.Set(da.Headers.V().Authenticate,
			fmt.Sprintf(`Digest realm="%s", nonce="%s", opaque="%s", algorithm=MD5, qop="auth"`,
				da.Realm, nonce, da.Opaque))
	}
	return context.WithValue(ctx, infoKey, info)
}

// NewDigestAuthenticator generates a new DigestAuth object
func NewDigestAuthenticator(realm string, secrets SecretProvider) *DigestAuth {
	da := &DigestAuth{
		Opaque:               RandomKey(),
		Realm:                realm,
		Secrets:              secrets,
		PlainTextSecrets:     false,
		ClientCacheSize:      DefaultClientCacheSize,
		ClientCacheTolerance: DefaultClientCacheTolerance,
		clients:              map[string]*digestClient{}}
	return da
}
