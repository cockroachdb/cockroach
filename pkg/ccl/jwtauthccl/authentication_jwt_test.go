// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package jwtauthccl

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
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
	set.Add(pubKey1)
	set.Add(pubKey2)

	return set, key1, key2
}

func createECDSAKey(t *testing.T, keyID string) jwk.Key {
	raw, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)
	key, err := jwk.New(raw)
	require.NoError(t, err)
	require.NoError(t, key.Set(jwk.KeyIDKey, keyID))
	require.NoError(t, key.Set(jwk.AlgorithmKey, jwa.ES384))
	return key
}

func createRSAKey(t *testing.T, keyID string) jwk.Key {
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	key, err := jwk.New(raw)
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
	signedTokenBytes, err := jwt.Sign(token, algorithm, key)
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
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: not enabled")

	// Enable JWT auth.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	// Now the validate call gets past the enabled check and fails on the next check (issuer check).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// When JWKSAutoFetchEnabled JWKS fetch should be attempted and  fail for configured issuer.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")

	// Set the JWKS cluster setting.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, jwkPublicKey)

	// Now the validate call gets past the token validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// When JWKSAutoFetchEnabled, JWKS fetch should be attempted and  fail for configured issuer.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")

	// Set the JWKS cluster setting.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, jwkPublicKey)

	// Now the validate call gets past the token validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer1)
	keySet, key, key2 := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key2, jwa.ES384, "", "")
	publicKey, err := key.PublicKey()
	require.NoError(t, err)
	keySetWithOneKey := jwk.NewSet()
	keySetWithOneKey.Add(publicKey)
	// Set the JWKS to only include jwk1.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySetWithOneKey))

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// When JWKSAutoFetchEnabled the jwks fetch should be attempted and fail.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: unable to validate token")

	// Set both jwk1 and jwk2 to be valid signing keys.
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))

	// Now jwk2 token passes the validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer1)
	keySet, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(-1*time.Second), key, jwa.RS256, "", "")
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens with an expiration date in the past.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid token")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer1)

	// When JWKSAutoFetchEnabled the jwks fetch should be attempted and fail.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Reset the key id and regenerate the token.
	require.NoError(t, key.Set(jwk.KeyIDKey, keyID1))
	token = createJWT(t, username1, audience1, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")
	// Now jwk1 token passes the validity check and fails on the next check (subject matching user)..
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
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
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token1, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")

	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)
	// Validation fails with an issuer error when the issuer in the token is not in cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token1, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")

	// Validation succeeds when the issuer in the token is equal to the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Set the cluster setting to accept issuer values of either "issuer" or "issuer2".
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, "[\""+issuer1+"\", \""+issuer2+"\"]")

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token1, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation passes the subject check when the username matches the subject and then fails on the next
	// check (audience field not matching).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)

	// Validation fails with missing claim
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), missingClaimToken, identMap)
	require.ErrorContains(t, err, "JWT authentication: missing claim")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)

	// the integer claim is implicitly cast to a string
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), intClaimToken, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation passes the subject check when the username matches the subject and then fails on the next
	// check (audience field not matching).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)
	JWTAuthClaim.Override(ctx, &s.ClusterSettings().SV, customClaimName)

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation passes the subject check when the username matches the subject and then fails on the next
	// check (audience field not matching).
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username2), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)

	// Validation fails with a subject error when a user tries to log in when their user is mapped to username2
	// but they try to log in with username1.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation fails if there is a map for the issuer but no mapping rule matches.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid principal")

	// Validation passes the subject check when the username matches the mapped subject.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username2), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, "[\""+issuer1+"\", \""+issuer2+"\"]")

	// You cannot log in as root or other reserved users using token based auth when mapped to root.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("root"), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid identity")

	// You cannot log in as root or other reserved users using token based auth when no map is involved.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("root"), token2, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid identity")
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
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)

	// Set audience field to audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience2)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation passes the audience check now that they match.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Set audience field to both audience1 and audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "[\""+audience2+"\",\""+audience1+"\"]")
	// Validation passes the audience check now that both audiences are accepted.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}

// mockGetHttpResponseWithLocalFileContent is a mock function for getHttpResponse. This is used to intercept the call to
// getHttpResponse and return the content of a local file instead of making a http call.
var mockGetHttpResponseWithLocalFileContent = func(ctx context.Context, url string) ([]byte, error) {
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

// test that jwks url is used when JWKSAutoFetchEnabled is true.
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
	// Matching public key available in jwks url is used to verify token.
	keySet := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySet.Get(0)
	validIssuer := "https://accounts.idp1.com"
	token := createJWT(t, username1, audience1, validIssuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	//JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, validIssuer)

	// Set audience field to audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience2)

	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation passes the audience check now that they match.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Set audience field to both audience1 and audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "[\""+audience2+"\",\""+audience1+"\"]")
	// Validation passes the audience check now that both audiences are accepted.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}

// test jwks url is used when JWKSAutoFetchEnabled and static jwks ignored.
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
	// Matching public key available in jwks url is used to verify token.
	keySetUsedForSigning := createJWKSFromFile(t, "testdata/www.idp1apis.com_oauth2_v3_certs_private")
	key, _ := keySetUsedForSigning.Get(0)
	validIssuer := "https://accounts.idp1.com"
	token := createJWT(t, username1, audience1, validIssuer, timeutil.Now().Add(time.Hour), key, jwa.RS256, "", "")

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWKSAutoFetchEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	// Configure cluster setting with a key that is not used for signing.
	keySetNotUsedForSigning, _, _ := createJWKS(t)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySetNotUsedForSigning))
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, validIssuer)

	// Set audience field to audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience2)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience1)

	// Validation passes the audience check now that they match.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)

	// Set audience field to both audience1 and audience2.
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "[\""+audience2+"\",\""+audience1+"\"]")
	// Validation passes the audience check now that both audiences are accepted.
	err = verifier.ValidateJWTLogin(ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token, identMap)
	require.NoError(t, err)
}
