// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/redact"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
)

var username1 = "test_user1"
var username2 = "test_user2"
var invalidUsername = "invalid_user"

var keyID1 = "test_kid1"
var keyID2 = "test_kid2"
var invalidKeyID = "invalid_key_id"

var audience1 = "test_cluster"
var audience2 = "audience_2"

var issuer1 = "issuer1"
var issuer2 = "issuer2"

var customClaimName = "groups"

func createJWKS(t *testing.T) (jwk.Set, jwk.Key, jwk.Key) {
	key1 := createRSAKey(t, keyID1)
	key2 := createECDSAKey(t, keyID2)
	pubKey1, err := key1.PublicKey()
	require.NoError(t, err)
	pubKey2, err := key2.PublicKey()
	require.NoError(t, err)
	set := jwk.NewSet()
	require.NoError(t, set.AddKey(pubKey1))
	require.NoError(t, set.AddKey(pubKey2))

	return set, key1, key2
}

func createECDSAKey(t *testing.T, keyID string) jwk.Key {
	raw, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)
	key, err := jwk.FromRaw(raw)
	require.NoError(t, err)
	require.NoError(t, key.Set(jwk.KeyIDKey, keyID))
	require.NoError(t, key.Set(jwk.AlgorithmKey, jwa.ES384))
	return key
}

func createRSAKey(t *testing.T, keyID string) jwk.Key {
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	key, err := jwk.FromRaw(raw)
	require.NoError(t, err)
	require.NoError(t, key.Set(jwk.KeyIDKey, keyID))
	require.NoError(t, key.Set(jwk.AlgorithmKey, jwa.RS256))
	return key
}

func createJWT(
	t *testing.T,
	subject string,
	audience string,
	issuer string,
	expiredAt time.Time,
	key jwk.Key,
	algorithm jwa.SignatureAlgorithm,
	customClaimName string,
	customClaimValue interface{},
) []byte {
	token := jwt.New()
	require.NoError(t, token.Set(jwt.SubjectKey, subject))
	require.NoError(t, token.Set(jwt.AudienceKey, audience))
	require.NoError(t, token.Set(jwt.IssuerKey, issuer))
	require.NoError(t, token.Set(jwt.ExpirationKey, expiredAt))
	if customClaimName != "" {
		require.NoError(t, token.Set(customClaimName, customClaimValue))
	}
	signedTokenBytes, err := jwt.Sign(token, jwt.WithKey(algorithm, key))
	require.NoError(t, err)
	return signedTokenBytes
}

func serializePublicKey(t *testing.T, key jwk.Key) string {
	jsonbuf, err := json.Marshal(key)
	require.NoError(t, err)
	return string(jsonbuf)
}

func serializePublicKeySet(t *testing.T, set jwk.Set) string {
	jsonbuf, err := json.Marshal(set)
	require.NoError(t, err)
	return string(jsonbuf)
}

func TestJWTEnabledCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	key := createRSAKey(t, keyID1)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	// JWT auth is not enabled.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: not enabled")

	// Enable JWT auth.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	// Now the validate call gets past the enabled check and fails on the next check (issuer check).
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
	require.EqualValues(t, "token issued by issuer1", errors.GetAllDetails(err)[0])
}

func TestJWTSingleKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	_, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	publicKey, err := key.PublicKey()
	require.NoError(t, err)
	jwkPublicKey := serializePublicKey(t, publicKey)

	// Configure issuer as it gets checked even before the token validity check.
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// When JWKSAutoFetchEnabled JWKS fetch should be attempted and  fail for configured issuer.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	detailedErrorMsg, err := verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")
	require.EqualValues(t, redact.RedactableString(`unable to fetch jwks: ‹Get "issuer1/.well-known/openid-configuration"›: ‹unsupported protocol scheme ""›`), detailedErrorMsg)

	// Set the JWKS cluster setting.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, jwkPublicKey)

	// Now the validate call gets past the token validity check and fails on the next check (subject matching user).
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])
}

func TestJWTSingleKeyWithoutKeyAlgorithm(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	_, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	// Clear the algorithm.
	require.NoError(t, key.Remove(jwk.AlgorithmKey))
	publicKey, err := key.PublicKey()
	require.NoError(t, err)
	jwkPublicKey := serializePublicKey(t, publicKey)

	// Configure issuer as it gets checked even before the token validity check.
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// When JWKSAutoFetchEnabled, JWKS fetch should be attempted and  fail for configured issuer.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")

	// Set the JWKS cluster setting.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, jwkPublicKey)

	// Now the validate call gets past the token validity check and fails on the next check (subject matching user).
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])
}

func TestJWTMultiKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	// Make sure jwt auth is enabled.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	// Configure issuer as it gets checked even before the token validity check.
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer1)
	keySet, key, key2 := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")
	publicKey, err := key.PublicKey()
	require.NoError(t, err)
	keySetWithOneKey := jwk.NewSet()
	require.NoError(t, keySetWithOneKey.AddKey(publicKey))
	// Set the JWKS to only include jwk1.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySetWithOneKey))

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// When JWKSAutoFetchEnabled the jwks fetch should be attempted and fail.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")

	// Set both jwk1 and jwk2 to be valid signing keys.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))

	// Now jwk2 token passes the validity check and fails on the next check (subject matching user).
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])
}

func TestExpiredToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)

	// Make sure jwt auth is enabled and accepts valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	// Configure issuer as it gets checked even before the token validity check.
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer1)
	keySet, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(-1*time.Second), key, jwa.RS256, "", "")
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens with an expiration date in the past.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid token")
	require.EqualValues(t, "unable to parse token: \"exp\" not satisfied", errors.GetAllDetails(err)[0])
}

func TestKeyIdMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, key, _ := createJWKS(t)
	// Create a JWT with different key id.
	require.NoError(t, key.Set(jwk.KeyIDKey, invalidKeyID))
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	// Make sure jwt auth is enabled and accepts valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	// Configure issuer as it gets checked even before the token validity check.
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// When JWKSAutoFetchEnabled the jwks fetch should be attempted and fail.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Reset the key id and regenerate the token.
	require.NoError(t, key.Set(jwk.KeyIDKey, keyID1))
	token = createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	// Now jwk1 token passes the validity check and fails on the next check (subject matching user)..
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}

func TestIssuerCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, key, _ := createJWKS(t)
	token1 := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	token2 := createJWT(t, username1, audience1, issuer2, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	// Make sure jwt auth is enabled, accepts jwk1 or jwk2 as valid signing keys, accepts the audience of "test_cluster"
	// and the issuer of "issuer2".
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with no issuer are configured.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token1, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
	require.EqualValues(t, "token issued by issuer1", errors.GetAllDetails(err)[0])

	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)
	// Validation fails with an issuer error when the issuer in the token is not in cluster's accepted issuers.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token1, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
	require.EqualValues(t, "token issued by issuer1", errors.GetAllDetails(err)[0])

	// Validation succeeds when the issuer in the token is equal to the cluster's accepted issuers.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])

	// Set the cluster setting to accept issuer values of either "issuer" or "issuer2".
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, "[\""+issuer1+"\", \""+issuer2+"\"]")

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token1, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])
}

func TestSubjectCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for invalid_user", errors.GetAllDetails(err)[0])

	// Validation passes the subject check when the username matches the subject and then fails on the next
	// check (audience field not matching).
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	require.EqualValues(t, "token issued with an audience of [test_cluster]", errors.GetAllDetails(err)[0])
}

func TestClaimMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	missingClaimToken := createJWT(t, invalidUsername, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)

	// Validation fails with missing claim
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), missingClaimToken, identMap)
	require.ErrorContains(t, err, "JWT authentication: missing claim")
	require.EqualValues(t, "token does not contain a claim for groups", errors.GetAllDetails(err)[0])
}

func TestIntegerClaimValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// map the value 1 to a valid user
	identMapString := issuer2 + "     1    " + username1
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	intClaimToken := createJWT(t, invalidUsername, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, customClaimName, 1)

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)

	// the integer claim is implicitly cast to a string
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), intClaimToken, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	require.EqualValues(t, "token issued with an audience of [test_cluster]", errors.GetAllDetails(err)[0])
}

func TestSingleClaim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	token := createJWT(t, invalidUsername, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, customClaimName, username1)

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test1".
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation is successful for a token with a matched principal.
	retrievedUser, err := verifier.RetrieveIdentity(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
	require.Equal(t, username.MakeSQLUsernameFromPreNormalizedString(username1), retrievedUser)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Validation is successful for a token without a username provided, as a single principal is matched.
	retrievedUser, err = verifier.RetrieveIdentity(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(""), token, identMap)
	require.NoError(t, err)
	require.Equal(t, username.MakeSQLUsernameFromPreNormalizedString(username1), retrievedUser)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(""), token, identMap)
	require.NoError(t, err)
}

func TestMultipleClaim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	token := createJWT(t, invalidUsername, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, customClaimName, []string{username2, username1})

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation is successful for a token with a matched principal - test1.
	retrievedUser, err := verifier.RetrieveIdentity(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
	require.Equal(t, username.MakeSQLUsernameFromPreNormalizedString(username1), retrievedUser)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Validation is successful for a token with a matched principal - test2.
	retrievedUser, err = verifier.RetrieveIdentity(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username2), token, identMap)
	require.NoError(t, err)
	require.Equal(t, username.MakeSQLUsernameFromPreNormalizedString(username2), retrievedUser)
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username2), token, identMap)
	require.NoError(t, err)

	// Validation fails for a token without a username provided, as multiple principals are matched.
	_, err = verifier.RetrieveIdentity(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(""), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(""), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
}

func TestSubjectMappingCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Create a map for issuer2 from username1 to username2 (note not the reverse).
	identMapString := issuer2 + "    " + username1 + "    " + username2
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")
	token2 := createJWT(t, username2, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)

	// Validation fails with a subject error when a user tries to log in when their user is mapped to username2
	// but they try to log in with username1.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "token issued for [test_user1] and login was for test_user1", errors.GetAllDetails(err)[0])

	// Validation fails if there is a map for the issuer but no mapping rule matches.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
	require.EqualValues(t, "the value [test_user2] for the issuer issuer2 is invalid", errors.GetAllDetails(err)[0])

	// Validation passes the subject check when the username matches the mapped subject.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username2), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	require.EqualValues(t, "token issued with an audience of [test_cluster]", errors.GetAllDetails(err)[0])
}

func TestSubjectReservedUser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Create a map for issuer2 from username1 to username2 (note not the reverse).
	identMapString := issuer2 + "    " + username1 + "    root"
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, _, key2 := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")
	token2 := createJWT(t, "root", audience1, issuer1, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, "[\""+issuer1+"\", \""+issuer2+"\"]")

	// You cannot log in as root or other reserved users using token based auth when mapped to root.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("root"), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid identity")
	require.EqualValues(t, "cannot use JWT auth to login to a reserved user root", errors.GetAllDetails(err)[0])

	// You cannot log in as root or other reserved users using token based auth when no map is involved.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("root"), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid identity")
	require.EqualValues(t, "cannot use JWT auth to login to a reserved user root", errors.GetAllDetails(err)[0])
}

func TestAudienceCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)
	keySet, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer2, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer2)

	// Set audience field to audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience2)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	require.EqualValues(t, "token issued with an audience of [test_cluster]", errors.GetAllDetails(err)[0])

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation passes the audience check now that they match.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Set audience field to both audience1 and audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "[\""+audience2+"\",\""+audience1+"\"]")
	// Validation passes the audience check now that both audiences are accepted.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}

// mockGetHttpResponseWithLocalFileContent is a mock function for getHttpResponse. This is used to intercept the call to
// getHttpResponse and return the content of a local file instead of making a http call.
var mockGetHttpResponseWithLocalFileContent = func(ctx context.Context, url string, authenticator *jwtAuthenticator, _ ...http.Header) ([]byte, error) {
	// remove https:// and replace / with _ in the url to get the testdata file name
	fileName := "testdata/" + strings.ReplaceAll(strings.ReplaceAll(url, "https://", ""), "/", "_")
	// read content of the file as a byte array
	byteValue, err := os.ReadFile(fileName)
	if err != nil {
		if oserror.IsNotExist(err) {
			// return http status 404 if the file does not exist
			return nil, errors.New("404 Not Found")
		}
		return nil, err
	}
	return byteValue, nil
}

// createJWKSFromFile creates a jwk set from a local file. The file used by this function is expected to contain both
// private and public keys.
func createJWKSFromFile(t *testing.T, fileName string) jwk.Set {
	byteValue, err := os.ReadFile(fileName)
	require.NoError(t, err)
	jwkSet, err := jwk.Parse(byteValue)
	if err != nil {
		return nil
	}
	return jwkSet
}

// test that jwks URI is used when JWKSAutoFetchEnabled is true.
func Test_JWKSFetchWorksWhenEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Intercept the call to getHttpResponse and return the mockGetHttpResponse
	restoreHook := testutils.TestingHook(&getHttpResponse, mockGetHttpResponseWithLocalFileContent)
	defer func() {
		restoreHook()
	}()
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)

	// Create key from a file. This key will be used to sign the token.
	// Matching public key available in jwks URI is used to verify token.
	keySet := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySet.Key(0)
	validIssuer := "https://accounts.idp1.com"
	token := createJWT(t, username1, audience1, validIssuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	//JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, validIssuer)

	// Set audience field to audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience2)

	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	require.EqualValues(t, "token issued with an audience of [test_cluster]", errors.GetAllDetails(err)[0])

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation passes the audience check now that they match.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Set audience field to both audience1 and audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "[\""+audience2+"\",\""+audience1+"\"]")
	// Validation passes the audience check now that both audiences are accepted.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}

// test jwks URI is used when JWKSAutoFetchEnabled and static jwks ignored.
func Test_JWKSFetchWorksWhenEnabledIgnoresTheStaticJWKS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Intercept the call to getHttpResponse and return the mockGetHttpResponse
	restoreHook := testutils.TestingHook(&getHttpResponse, mockGetHttpResponseWithLocalFileContent)
	defer func() {
		restoreHook()
	}()
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)

	// Create key from a file. This key will be used to sign the token.
	// Matching public key available in jwks URI is used to verify token.
	keySetUsedForSigning := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySetUsedForSigning.Key(0)
	validIssuer := "https://accounts.idp1.com"
	token := createJWT(t, username1, audience1, validIssuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	// Configure cluster setting with a key that is not used for signing.
	keySetNotUsedForSigning, _, _ := createJWKS(t)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySetNotUsedForSigning))
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, validIssuer)

	// Set audience field to audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience2)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	require.EqualValues(t, "token issued with an audience of [test_cluster]", errors.GetAllDetails(err)[0])

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation passes the audience check now that they match.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Set audience field to both audience1 and audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "[\""+audience2+"\",\""+audience1+"\"]")
	// Validation passes the audience check now that both audiences are accepted.
	_, err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}

func TestJWTAuthCanUseHTTPProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf("proxied-%s", r.URL)))
	}))
	defer proxy.Close()

	// Normally, we would set proxy via HTTP_PROXY environment variable.
	// However, if we run multiple tests in this package, and earlier tests
	// happen to create an http client, then the DefaultTransport will have
	// been been initialized with an empty Proxy.  So, set proxy directly.
	defer testutils.TestingHook(
		&http.DefaultTransport.(*http.Transport).Proxy,
		func(_ *http.Request) (*url.URL, error) {
			return url.Parse(proxy.URL)
		})()

	authenticator := jwtAuthenticator{}
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	authenticator.mu.conf.httpClient = httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout)

	res, err := getHttpResponse(ctx, "http://my-server/.well-known/openid-configuration", &authenticator)
	require.NoError(t, err)
	require.EqualValues(t, "proxied-http://my-server/.well-known/openid-configuration", string(res))
}

func TestJWTAuthWithCustomCACert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Initiate a test JWKS server locally over HTTPS.
	testServer := httptest.NewUnstartedServer(nil)
	testServerURL := "https://" + testServer.Listener.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /.well-known/openid-configuration",
		func(w http.ResponseWriter, r *http.Request) {
			// Serve the response locally from testdata.
			dataBytes, err := os.ReadFile("testdata/accounts.idp1.com_.well-known_openid-configuration")
			require.NoError(t, err)

			type providerJSON struct {
				Issuer      string   `json:"issuer"`
				AuthURL     string   `json:"authorization_endpoint"`
				TokenURL    string   `json:"token_endpoint"`
				JWKSURI     string   `json:"jwks_uri"`
				UserInfoURL string   `json:"userinfo_endpoint"`
				Algorithms  []string `json:"id_token_signing_alg_values_supported"`
			}

			var p providerJSON
			err = json.Unmarshal(dataBytes, &p)
			require.NoError(t, err)

			// We need to update the 'jwks_uri' to point to the local test server.
			p.JWKSURI = testServerURL + "/jwks"

			updatedBytes, err := json.Marshal(p)
			require.NoError(t, err)
			_, err = w.Write(updatedBytes)
			require.NoError(t, err)
		},
	)
	mux.HandleFunc(
		"GET /jwks",
		func(w http.ResponseWriter, r *http.Request) {
			// Serve the JWKS response locally from testdata.
			dataBytes, err := os.ReadFile("testdata/www.idp1apis.com_oauth2_v3_certs")
			require.NoError(t, err)
			_, err = w.Write(dataBytes)
			require.NoError(t, err)
		},
	)

	testServer.Config = &http.Server{
		Handler: mux,
	}

	certPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedNodeCert))
	require.NoError(t, err)
	keyPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedNodeKey))
	require.NoError(t, err)
	tlsCert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	require.NoError(t, err)

	testServer.TLS = &tls.Config{Certificates: []tls.Certificate{tlsCert}}
	testServer.StartTLS()
	defer testServer.Close()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)

	// Create a key to sign the token using testdata.
	// The same will be fetched through the JWKS URI to verify the token.
	keySet := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySet.Key(0)
	issuer := testServerURL
	token := createJWT(
		t, username1, audience1, issuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer)
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	for _, testCase := range []struct {
		testName string
		// caCertName is the name of the certificate looked up within `test_certs`.
		// Empty value is treated as no certificate.
		caCertName string
		assertFn   func(t require.TestingT, err error, msgAndArgs ...interface{})
	}{
		{
			testName:   "fail if no CA certificate is provided",
			caCertName: "",
			assertFn:   require.Error,
		},
		{
			testName:   "fail if an incorrect CA certificate is provided",
			caCertName: certnames.EmbeddedTestUserCert,
			assertFn:   require.Error,
		},
		{
			testName:   "success if the correct CA certificate is provided",
			caCertName: certnames.EmbeddedCACert,
			assertFn:   require.NoError,
		},
	} {
		t.Run(testCase.testName, func(t *testing.T) {
			if testCase.caCertName != "" {
				publicKeyPEM, err := securityassets.GetLoader().ReadFile(
					filepath.Join(certnames.EmbeddedCertsDir, testCase.caCertName))
				require.NoError(t, err)
				JWTAuthIssuerCustomCA.Override(ctx, &s.ClusterSettings().SV, string(publicKeyPEM))
			}

			_, err = verifier.ValidateJWTLogin(
				ctx,
				s.ClusterSettings(),
				username.MakeSQLUsernameFromPreNormalizedString(username1),
				token,
				identMap,
			)
			testCase.assertFn(t, err)
		})
	}
}

func TestJWTAuthClientTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Initiate a test JWKS server locally.
	testServer := httptest.NewUnstartedServer(nil)
	waitChan := make(chan struct{}, 1)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /jwks",
		func(w http.ResponseWriter, r *http.Request) {
			// Hang the request handler to enforce HTTP client timeout.
			<-waitChan
		},
	)

	testServer.Config = &http.Server{
		Handler: mux,
	}
	testServer.Start()
	defer func() {
		waitChan <- struct{}{}
		close(waitChan)
		testServer.Close()
	}()

	mockGetHttpResponse := func(ctx context.Context, url string, authenticator *jwtAuthenticator, _ ...http.Header) ([]byte, error) {
		if strings.Contains(url, "/.well-known/openid-configuration") {
			return mockGetHttpResponseWithLocalFileContent(ctx, url, authenticator)
		} else if strings.Contains(url, "/oauth2/v3/certs") {
			// For fetching JWKS, point to the local test server.
			resp, err := authenticator.mu.conf.httpClient.Get(
				context.Background(),
				testServer.URL+"/jwks",
			)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			return body, nil
		}
		return nil, errors.Newf("unsupported route: %s", url)
	}
	getHttpResponseTestHook := testutils.TestingHook(&getHttpResponse, mockGetHttpResponse)
	defer getHttpResponseTestHook()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)

	// Create a key to sign the token using testdata.
	// The same will be fetched through the JWKS URI to verify the token.
	keySet := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySet.Key(0)
	validIssuer := "https://accounts.idp1.com"
	token := createJWT(
		t, username1, audience1, validIssuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, validIssuer)
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)
	JWTAuthClientTimeout.Override(ctx, &s.ClusterSettings().SV, time.Millisecond)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	errMsg, err := verifier.ValidateJWTLogin(
		ctx,
		s.ClusterSettings(),
		username.MakeSQLUsernameFromPreNormalizedString(username1),
		token,
		identMap,
	)
	require.Regexp(
		t,
		regexp.MustCompile(`unable to fetch jwks:.*\(Client.Timeout exceeded while awaiting headers\)`),
		errMsg,
	)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")
}

func TestJWTAuthWithIssuerJWKSConfAutoFetchJWKS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Initiate a test OIDC/JWKS server locally over HTTP.
	testServer := httptest.NewUnstartedServer(nil)
	testServerURL := "http://" + testServer.Listener.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /.well-known/openid-configuration",
		func(w http.ResponseWriter, r *http.Request) {
			// Serve the response locally mocking openid config.
			dataBytes := map[string]string{"invalid-key": "invalid-value"}
			updatedBytes, _ := json.Marshal(dataBytes)
			_, err := w.Write(updatedBytes)
			require.NoError(t, err)
		},
	)
	mux.HandleFunc(
		"GET /jwks",
		func(w http.ResponseWriter, r *http.Request) {
			// Serve the JWKS response locally from testdata.
			dataBytes, err := os.ReadFile("testdata/www.idp1apis.com_oauth2_v3_certs")
			require.NoError(t, err)
			_, err = w.Write(dataBytes)
			require.NoError(t, err)
		},
	)

	testServer.Config = &http.Server{
		Handler: mux,
	}
	testServer.Start()
	defer testServer.Close()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a key to sign the token using testdata.
	// The same will be fetched through the JWKS URI to verify the token.
	keySet := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySet.Key(0)
	issuer := testServerURL
	token := createJWT(
		t, username1, audience1, issuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	identMapString := ""
	identMap, err := identmap.From(strings.NewReader(identMapString))
	require.NoError(t, err)

	for _, testCase := range []struct {
		testName           string
		issuerConfSetting  string
		assertFn           func(t require.TestingT, err error, msgAndArgs ...interface{})
		expectedErr        string
		expectedErrDetails string
		detailedErrMsg     string
	}{
		{
			testName:           "fail if issuer not set",
			issuerConfSetting:  "",
			assertFn:           require.Error,
			expectedErr:        "JWT authentication: invalid issuer",
			expectedErrDetails: "token issued by " + testServerURL,
		},
		{
			testName:          "fail if issuer provided without JWKS URI mapping",
			issuerConfSetting: testServerURL,
			assertFn:          require.Error,
			expectedErr:       "JWT authentication: unable to validate token",
			detailedErrMsg:    "unable to fetch jwks: no JWKS URI found in OpenID configuration",
		},
		{
			testName:          "success if issuer to jwks URI provided",
			issuerConfSetting: "{\"issuer_jwks_map\": {\"" + testServerURL + "\": \"" + testServerURL + "/jwks" + "\"}}",
			assertFn:          require.NoError,
		},
	} {
		t.Run(testCase.testName, func(t *testing.T) {
			// set the issuer configuration cluster setting
			JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, testCase.issuerConfSetting)
			detailedErrMsg, err := verifier.ValidateJWTLogin(
				ctx,
				s.ClusterSettings(),
				username.MakeSQLUsernameFromPreNormalizedString(username1),
				token,
				identMap,
			)
			testCase.assertFn(t, err)
			if err != nil {
				require.Equal(t, testCase.expectedErr, err.Error())
				require.Equal(t, testCase.expectedErrDetails, errors.FlattenDetails(err))
				require.Equal(t, detailedErrMsg, redact.RedactableString(testCase.detailedErrMsg))
			}
		})
	}
}

func TestVerifyAndExtractIssuer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Enable JWT and configure a single issuer.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// Create signing material.
	keySet, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Succeeds with correct issuer & signature.
	iss, detail, err := verifier.VerifyAndExtractIssuer(ctx, s.ClusterSettings(), token)
	require.NoError(t, err)
	require.Empty(t, detail)
	require.Equal(t, issuer1, iss)

	// Fails on wrong issuer.
	badIssuerToken := createJWT(t, username1, audience1, issuer2, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	_, detail, err = verifier.VerifyAndExtractIssuer(ctx, s.ClusterSettings(), badIssuerToken)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
	require.Contains(t, errors.FlattenDetails(err), "token issued by issuer2")
	require.Empty(t, detail)

	// Fails on bad signature (different key).
	_, _, otherKey := createJWKS(t)
	badSigToken := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), otherKey, jwa.ES384, "", "")
	_, detail, err = verifier.VerifyAndExtractIssuer(ctx, s.ClusterSettings(), badSigToken)
	require.ErrorContains(t, err, "JWT authentication: invalid token")
	require.Empty(t, detail)
}

func TestGetHTTPResponseNon2xx(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusForbidden)
	}))
	defer ts.Close()

	authenticator := &jwtAuthenticator{}
	authenticator.mu.conf.httpClient = httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout)

	_, err := getHttpResponse(context.Background(), ts.URL, authenticator)
	require.Error(t, err)
	require.Equal(t, "JWT authentication: HTTP request failed", err.Error())
	require.Contains(t, errors.FlattenDetails(err), "403 Forbidden")
	require.Contains(t, errors.FlattenDetails(err), "boom")
}

func TestLookupIdentityClaimByJSONPointer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Build a token with a mix of flat and nested claims for the test cases.
	tok := makeTokenWithClaims(t, map[string]any{
		jwt.SubjectKey: username1,
		jwt.IssuerKey:  issuer1,
		"email":        "alice@example.com",
		"count":        42,
		"user": map[string]any{
			"name": "alice",
			"profile": map[string]any{
				"role": "admin",
			},
		},
		"roles": []any{"viewer", "editor", "admin"},
		"nested_arr": map[string]any{
			"items": []any{
				map[string]any{"id": "first"},
				map[string]any{"id": "second"},
			},
		},
		// Keys with characters that require RFC 6901 escaping.
		"a/b":   "slash-value",
		"a~c":   "tilde-value",
		"~0":    "literal-tilde-zero",
		"~1":    "literal-tilde-one",
		"empty": "",
	})

	type testCase struct {
		name      string
		pointer   string
		wantValue interface{}
		wantOK    bool
		wantErr   bool
	}

	// ── Simple claim names use token.Get directly (production path) ──
	t.Run("simple_claim_found", func(t *testing.T) {
		v, ok := tok.Get("email")
		require.True(t, ok)
		require.Equal(t, "alice@example.com", v)
	})
	t.Run("simple_claim_standard_field", func(t *testing.T) {
		v, ok := tok.Get("sub")
		require.True(t, ok)
		require.Equal(t, username1, v)
	})
	t.Run("simple_claim_integer", func(t *testing.T) {
		v, ok := tok.Get("count")
		require.True(t, ok)
		require.Equal(t, 42, v)
	})
	t.Run("simple_claim_empty_string", func(t *testing.T) {
		v, ok := tok.Get("empty")
		require.True(t, ok)
		require.Equal(t, "", v)
	})
	t.Run("simple_claim_missing", func(t *testing.T) {
		_, ok := tok.Get("nonexistent")
		require.False(t, ok)
	})

	cases := []testCase{
		// ── JSON Pointers ──
		{
			name:      "pointer_shallow",
			pointer:   "/email",
			wantValue: "alice@example.com",
			wantOK:    true,
		},
		{
			name:      "pointer_nested_one_level",
			pointer:   "/user/name",
			wantValue: "alice",
			wantOK:    true,
		},
		{
			name:      "pointer_nested_two_levels",
			pointer:   "/user/profile/role",
			wantValue: "admin",
			wantOK:    true,
		},
		{
			name:    "pointer_missing_leaf",
			pointer: "/user/missing",
			wantOK:  false,
			wantErr: true,
		},
		{
			name:    "pointer_missing_intermediate",
			pointer: "/nonexistent/deep/path",
			wantOK:  false,
			wantErr: true,
		},
		{
			name:    "pointer_non_object_intermediate",
			pointer: "/email/something",
			wantOK:  false,
			wantErr: true,
		},
		{
			// RFC 6901: "~1" decodes to "/", so this looks up the key "a/b".
			name:      "pointer_rfc6901_escaped_slash",
			pointer:   "/a~1b",
			wantValue: "slash-value",
			wantOK:    true,
		},
		{
			// RFC 6901: "~0" decodes to "~", so this looks up the key "a~c".
			name:      "pointer_rfc6901_escaped_tilde",
			pointer:   "/a~0c",
			wantValue: "tilde-value",
			wantOK:    true,
		},
		{
			// Key is literally "~0": encode "~" → "~0", "0" stays → "~00".
			name:      "pointer_rfc6901_literal_tilde_zero",
			pointer:   "/~00",
			wantValue: "literal-tilde-zero",
			wantOK:    true,
		},
		{
			// Key is literally "~1": encode "~" → "~0", "1" stays → "~01".
			name:      "pointer_rfc6901_literal_tilde_one",
			pointer:   "/~01",
			wantValue: "literal-tilde-one",
			wantOK:    true,
		},

		// ── Case (c): JSON arrays (RFC 6901 §4) ──
		{
			name:      "array_first_element",
			pointer:   "/roles/0",
			wantValue: "viewer",
			wantOK:    true,
		},
		{
			name:      "array_last_element",
			pointer:   "/roles/2",
			wantValue: "admin",
			wantOK:    true,
		},
		{
			name:    "array_index_out_of_bounds",
			pointer: "/roles/5",
			wantOK:  false,
			wantErr: true,
		},
		{
			name:    "array_index_negative",
			pointer: "/roles/-",
			wantOK:  false,
			wantErr: true,
		},
		{
			name:    "array_index_non_numeric",
			pointer: "/roles/abc",
			wantOK:  false,
			wantErr: true,
		},
		{
			name:    "array_index_leading_zero",
			pointer: "/roles/01",
			wantOK:  false,
			wantErr: true,
		},
		{
			name:      "array_nested_map_inside_array",
			pointer:   "/nested_arr/items/1/id",
			wantValue: "second",
			wantOK:    true,
		},
	}

	// Dedicated test for Kubernetes ServiceAccount tokens, the primary
	// motivation for JSON Pointer support. The serviceaccount UID lives at
	// /kubernetes.io/serviceaccount/uid inside the token.
	k8sTok := makeTokenWithClaims(t, map[string]any{
		jwt.AudienceKey: []string{"cockroachdb"},
		jwt.IssuerKey:   "https://my-issuer/...",
		jwt.SubjectKey:  "system:serviceaccount:my-namespace:my-service-account",
		"kubernetes.io": map[string]any{
			"namespace": "my-namespace",
			"node": map[string]any{
				"name": "ip-10-176-16-155.eu-central-1.compute.internal",
				"uid":  "6c9b214a-b6d8-43f9-b1e0-0e15b6126eda",
			},
			"pod": map[string]any{
				"name": "my-workload-54b8897d8-88wzg",
				"uid":  "c33ed74a-738c-499a-96d1-0bfdb68fee91",
			},
			"serviceaccount": map[string]any{
				"name": "my-service-account",
				"uid":  "136f6af7-4e5a-4891-b778-992ad1674b02",
			},
			"warnafter": 1772700468,
		},
	})
	k8sCases := []testCase{
		{
			name:      "k8s_serviceaccount_uid",
			pointer:   "/kubernetes.io/serviceaccount/uid",
			wantValue: "136f6af7-4e5a-4891-b778-992ad1674b02",
			wantOK:    true,
		},
		{
			name:      "k8s_serviceaccount_name",
			pointer:   "/kubernetes.io/serviceaccount/name",
			wantValue: "my-service-account",
			wantOK:    true,
		},
		{
			name:      "k8s_pod_uid",
			pointer:   "/kubernetes.io/pod/uid",
			wantValue: "c33ed74a-738c-499a-96d1-0bfdb68fee91",
			wantOK:    true,
		},
		{
			name:      "k8s_namespace",
			pointer:   "/kubernetes.io/namespace",
			wantValue: "my-namespace",
			wantOK:    true,
		},
		{
			name:    "k8s_missing_nested_key",
			pointer: "/kubernetes.io/serviceaccount/nonexistent",
			wantOK:  false,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			value, ok, err := lookupIdentityClaimByJSONPointer(tok, mustParseClaimSegments(tc.pointer))
			if tc.wantErr {
				require.Error(t, err)
				require.False(t, ok)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantValue, value)
			}
		})
	}

	for _, tc := range k8sCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			value, ok, err := lookupIdentityClaimByJSONPointer(k8sTok, mustParseClaimSegments(tc.pointer))
			if tc.wantErr {
				require.Error(t, err)
				require.False(t, ok)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantValue, value)
			}
		})
	}
}

func TestLookupIdentityClaim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tok := makeTokenWithClaims(t, map[string]any{
		jwt.SubjectKey: username1,
		jwt.IssuerKey:  issuer1,
		"email":        "alice@example.com",
		"user": map[string]any{
			"name": "nested-alice",
		},
	})

	t.Run("simple_claim_found", func(t *testing.T) {
		auth := &jwtAuthenticator{}
		auth.mu.conf.claim = "email"
		auth.mu.conf.claimSegments = mustParseClaimSegments("email") // nil

		v, ok, err := auth.lookupIdentityClaim(tok)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "alice@example.com", v)
	})

	t.Run("simple_claim_missing", func(t *testing.T) {
		auth := &jwtAuthenticator{}
		auth.mu.conf.claim = "nonexistent"
		auth.mu.conf.claimSegments = mustParseClaimSegments("nonexistent") // nil

		_, ok, err := auth.lookupIdentityClaim(tok)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("json_pointer_found", func(t *testing.T) {
		auth := &jwtAuthenticator{}
		auth.mu.conf.claim = "/user/name"
		auth.mu.conf.claimSegments = mustParseClaimSegments("/user/name")

		v, ok, err := auth.lookupIdentityClaim(tok)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "nested-alice", v)
	})

	t.Run("json_pointer_missing", func(t *testing.T) {
		auth := &jwtAuthenticator{}
		auth.mu.conf.claim = "/user/missing"
		auth.mu.conf.claimSegments = mustParseClaimSegments("/user/missing")

		_, ok, err := auth.lookupIdentityClaim(tok)
		require.Error(t, err)
		require.False(t, ok)
		require.ErrorContains(t, err, "JWT authentication: failed to extract claim")
	})

	t.Run("json_pointer_non_traversable", func(t *testing.T) {
		auth := &jwtAuthenticator{}
		auth.mu.conf.claim = "/email/deep"
		auth.mu.conf.claimSegments = mustParseClaimSegments("/email/deep")

		_, ok, err := auth.lookupIdentityClaim(tok)
		require.Error(t, err)
		require.False(t, ok)
		require.ErrorContains(t, err, "JWT authentication: failed to extract claim")
	})
}
