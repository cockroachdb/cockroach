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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lestrrat-go/jwx/jwa"
	jwk "github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/stretchr/testify/require"
)

var username1 = "test_user1"
var invalidUsername = "invalid_user"

var keyID1 = "test_kid1"
var keyID2 = "test_kid2"
var invalidKeyID = "invalid_key_id"

var audience = "test_cluster"

var issuer1 = "issuer1"
var issuer2 = "issuer2"

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
) []byte {
	token := jwt.New()
	require.NoError(t, token.Set(jwt.SubjectKey, subject))
	require.NoError(t, token.Set(jwt.AudienceKey, audience))
	require.NoError(t, token.Set(jwt.IssuerKey, issuer))
	require.NoError(t, token.Set(jwt.ExpirationKey, expiredAt))
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	key := createRSAKey(t, keyID1)
	token := createJWT(t, username1, audience, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256)
	// JWT auth is not enabled.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token)
	require.ErrorContains(t, err, "JWT authentication: not enabled")

	// Enable JWT auth.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	// Now the validate call gets past the enabled check and fails on the next check (token validity).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token)
	require.ErrorContains(t, err, "JWT authentication: invalid token")
}

func TestJWTSingleKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	_, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256)
	publicKey, err := key.PublicKey()
	require.NoError(t, err)
	jwkPublicKey := serializePublicKey(t, publicKey)

	// When no JWKS is specified the token will be invalid.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Set the JWKS cluster setting.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, jwkPublicKey)

	// Now the validate call gets past the token validity check and fails on the next check (subject matching user)
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestJWTMultiKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	keySet, key, key2 := createJWKS(t)
	token := createJWT(t, username1, audience, issuer1, timeutil.Now().Add(time.Hour), key2, jwa.ES384)
	publicKey, err := key.PublicKey()
	require.NoError(t, err)
	keySetWithOneKey := jwk.NewSet()
	keySetWithOneKey.Add(publicKey)
	// Set the JWKS to only include jwk1.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySetWithOneKey))

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens signed with a different key.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Set both jwk1 and jwk2 to be valid signing keys.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))

	// Now jwk2 token passes the validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestExpiredToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled and accepts valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	keySet, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience, issuer1, timeutil.Now().Add(-1*time.Second), key, jwa.RS256)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens with an expiration date in the past.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token)
	require.ErrorContains(t, err, "JWT authentication: invalid token")
}

func TestKeyIdMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	keySet, key, _ := createJWKS(t)
	// create JWT with different key id
	require.NoError(t, key.Set(jwk.KeyIDKey, invalidKeyID))
	token := createJWT(t, username1, audience, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256)
	// Make sure jwt auth is enabled and accepts valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens with a kid not equal to that in JWKS.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Reset key id and regenerate token
	require.NoError(t, key.Set(jwk.KeyIDKey, keyID1))
	token = createJWT(t, username1, audience, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256)
	// Now jwk1 token passes the validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestSubjectCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	keySet, _, key2 := createJWKS(t)
	token := createJWT(t, username1, audience, issuer2, timeutil.Now().Add(time.Hour), key2, jwa.ES384)

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(invalidUsername), token)
	require.ErrorContains(t, err, "JWT authentication: invalid subject")

	// Validation passes the subject check when the username matches the subject and then fails on the next
	// check (audience field not matching).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
}

func TestAudienceCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	keySet, key, _ := createJWKS(t)
	token := createJWT(t, username1, audience, issuer2, timeutil.Now().Add(time.Hour), key, jwa.RS256)

	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token)
	require.ErrorContains(t, err, "JWT authentication: invalid audience")

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience)

	// Validation passess the issuer check now that they match and fails on the next check (issuer field not matching).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
}

func TestIssuerCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	keySet, key, _ := createJWKS(t)
	token1 := createJWT(t, username1, audience, issuer1, timeutil.Now().Add(time.Hour), key, jwa.RS256)
	token2 := createJWT(t, username1, audience, issuer2, timeutil.Now().Add(time.Hour), key, jwa.RS256)

	// Make sure jwt auth is enabled, accepts jwk1 or jwk2 as valid signing keys, accepts the audience of "test_cluster"
	// and the issuer of "issuer2".
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, serializePublicKeySet(t, keySet))
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, audience)
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, issuer2)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the issuer in the token is equal to the cluster's accepted issuers.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token1)
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")

	// Validation succeeds when the issuer in the token is equal to the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token2)
	require.NoError(t, err)

	// Set the cluster setting to accept issuer values of either "issuer" or "issuer2".
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, "[\""+issuer1+"\", \""+issuer2+"\"]")

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token1)
	require.NoError(t, err)

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(username1), token2)
	require.NoError(t, err)

}
