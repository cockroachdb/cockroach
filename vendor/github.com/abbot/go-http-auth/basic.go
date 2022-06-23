package auth

import (
	"bytes"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/context"
)

type compareFunc func(hashedPassword, password []byte) error

var (
	errMismatchedHashAndPassword = errors.New("mismatched hash and password")

	compareFuncs = []struct {
		prefix  string
		compare compareFunc
	}{
		{"", compareMD5HashAndPassword}, // default compareFunc
		{"{SHA}", compareShaHashAndPassword},
		// Bcrypt is complicated. According to crypt(3) from
		// crypt_blowfish version 1.3 (fetched from
		// http://www.openwall.com/crypt/crypt_blowfish-1.3.tar.gz), there
		// are three different has prefixes: "$2a$", used by versions up
		// to 1.0.4, and "$2x$" and "$2y$", used in all later
		// versions. "$2a$" has a known bug, "$2x$" was added as a
		// migration path for systems with "$2a$" prefix and still has a
		// bug, and only "$2y$" should be used by modern systems. The bug
		// has something to do with handling of 8-bit characters. Since
		// both "$2a$" and "$2x$" are deprecated, we are handling them the
		// same way as "$2y$", which will yield correct results for 7-bit
		// character passwords, but is wrong for 8-bit character
		// passwords. You have to upgrade to "$2y$" if you want sant 8-bit
		// character password support with bcrypt. To add to the mess,
		// OpenBSD 5.5. introduced "$2b$" prefix, which behaves exactly
		// like "$2y$" according to the same source.
		{"$2a$", bcrypt.CompareHashAndPassword},
		{"$2b$", bcrypt.CompareHashAndPassword},
		{"$2x$", bcrypt.CompareHashAndPassword},
		{"$2y$", bcrypt.CompareHashAndPassword},
	}
)

// BasicAuth is an authenticator implementation for 'Basic' HTTP
// Authentication scheme (RFC 7617).
type BasicAuth struct {
	Realm   string
	Secrets SecretProvider
	// Headers used by authenticator. Set to ProxyHeaders to use with
	// proxy server. When nil, NormalHeaders are used.
	Headers *Headers
}

// check that BasicAuth implements AuthenticatorInterface
var _ = (AuthenticatorInterface)((*BasicAuth)(nil))

// CheckAuth checks the username/password combination from the
// request. Returns either an empty string (authentication failed) or
// the name of the authenticated user.
func (a *BasicAuth) CheckAuth(r *http.Request) string {
	user, password, ok := r.BasicAuth()
	if !ok {
		return ""
	}

	secret := a.Secrets(user, a.Realm)
	if secret == "" {
		return ""
	}

	if !CheckSecret(password, secret) {
		return ""
	}

	return user
}

// CheckSecret returns true if the password matches the encrypted
// secret.
func CheckSecret(password, secret string) bool {
	compare := compareFuncs[0].compare
	for _, cmp := range compareFuncs[1:] {
		if strings.HasPrefix(secret, cmp.prefix) {
			compare = cmp.compare
			break
		}
	}
	return compare([]byte(secret), []byte(password)) == nil
}

func compareShaHashAndPassword(hashedPassword, password []byte) error {
	d := sha1.New()
	d.Write(password)
	if subtle.ConstantTimeCompare(hashedPassword[5:], []byte(base64.StdEncoding.EncodeToString(d.Sum(nil)))) != 1 {
		return errMismatchedHashAndPassword
	}
	return nil
}

func compareMD5HashAndPassword(hashedPassword, password []byte) error {
	parts := bytes.SplitN(hashedPassword, []byte("$"), 4)
	if len(parts) != 4 {
		return errMismatchedHashAndPassword
	}
	magic := []byte("$" + string(parts[1]) + "$")
	salt := parts[2]
	if subtle.ConstantTimeCompare(hashedPassword, MD5Crypt(password, salt, magic)) != 1 {
		return errMismatchedHashAndPassword
	}
	return nil
}

// RequireAuth is an http.HandlerFunc for BasicAuth which initiates
// the authentication process (or requires reauthentication).
func (a *BasicAuth) RequireAuth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(contentType, a.Headers.V().UnauthContentType)
	w.Header().Set(a.Headers.V().Authenticate, `Basic realm="`+a.Realm+`"`)
	w.WriteHeader(a.Headers.V().UnauthCode)
	w.Write([]byte(a.Headers.V().UnauthResponse))
}

// Wrap returns an http.HandlerFunc, which wraps
// AuthenticatedHandlerFunc with this BasicAuth authenticator's
// authentication checks. Once the request contains valid credentials,
// it calls wrapped AuthenticatedHandlerFunc.
//
// Deprecated: new code should use NewContext instead.
func (a *BasicAuth) Wrap(wrapped AuthenticatedHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if username := a.CheckAuth(r); username == "" {
			a.RequireAuth(w, r)
		} else {
			ar := &AuthenticatedRequest{Request: *r, Username: username}
			wrapped(w, ar)
		}
	}
}

// NewContext returns a context carrying authentication information for the request.
func (a *BasicAuth) NewContext(ctx context.Context, r *http.Request) context.Context {
	info := &Info{Username: a.CheckAuth(r), ResponseHeaders: make(http.Header)}
	info.Authenticated = (info.Username != "")
	if !info.Authenticated {
		info.ResponseHeaders.Set(a.Headers.V().Authenticate, `Basic realm="`+a.Realm+`"`)
	}
	return context.WithValue(ctx, infoKey, info)
}

// NewBasicAuthenticator returns a BasicAuth initialized with provided
// realm and secrets.
//
// Deprecated: new code should construct BasicAuth values directly.
func NewBasicAuthenticator(realm string, secrets SecretProvider) *BasicAuth {
	return &BasicAuth{Realm: realm, Secrets: secrets}
}
