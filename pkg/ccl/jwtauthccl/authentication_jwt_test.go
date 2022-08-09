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
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

var jwk1 = "{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test\", \"n\": \"sJCwOk5gVjZZu3oaODecZaT_-Lee7J-q3rQIvCilg-7B8fFNJ2XHZCsF74JX2d7ePyjz7u9d2r5CvstufiH0qGPHBBm0aKrxGRILRGUTfqBs8Dnrnv9ymTEFsRUQjgy9ACUfwcgLVQIwv1NozySLb4Z5N8X91b0TmcJun6yKjBrnr1ynUsI_XXjzLnDpJ2Ng_shuj-z7DKSEeiFUg9eSFuTeg_wuHtnnhw4Y9pwT47c-XBYnqtGYMADSVEzKLQbUini0p4-tfYboF6INluKQsO5b1AZaaXgmStPIqteS7r2eR3LFL-XB7rnZOR4cAla773Cq5DD-8RnYamnmmLu_gQ\", \"e\": \"AQAB\"}"
var jwk2 = "{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test2\", \"n\": \"3gOrVdePypBAs6bTwD-6dZhMuwOSq8QllMihBfcsiRmo3c14_wfa_DRDy3kSsacwdih5-CaeF8ou-Dan6WqXzjDyJNekmGltPLfO2XB5FkHQoZ-X9lnXktsAgNLj3WsKjr-xUxrh8p8FFz62HJYN8QGaNttWBJZb3CgdzF7i8bPqVet4P1ekzs7mPBH2arEDy1f1q4o7fpmw0t9wuCrmtkj_g_eS6Hi2Rxm3m7HJUFVVbQeuZlT_W84FUzpSQCkNi2QDvoNVVCE2DSYZxDrzRxSZSv_fIh5XeJhwYY-f8iEfI4qx91ONGzGMvPn2GagrBnLBQRx-6RsORh4YmOOeeQ\", \"e\": \"AQAB\"}"

var jwk1WithWrongKid = "{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"avaluewedontexpect\", \"n\": \"sJCwOk5gVjZZu3oaODecZaT_-Lee7J-q3rQIvCilg-7B8fFNJ2XHZCsF74JX2d7ePyjz7u9d2r5CvstufiH0qGPHBBm0aKrxGRILRGUTfqBs8Dnrnv9ymTEFsRUQjgy9ACUfwcgLVQIwv1NozySLb4Z5N8X91b0TmcJun6yKjBrnr1ynUsI_XXjzLnDpJ2Ng_shuj-z7DKSEeiFUg9eSFuTeg_wuHtnnhw4Y9pwT47c-XBYnqtGYMADSVEzKLQbUini0p4-tfYboF6INluKQsO5b1AZaaXgmStPIqteS7r2eR3LFL-XB7rnZOR4cAla773Cq5DD-8RnYamnmmLu_gQ\", \"e\": \"AQAB\"}"

var jwk1Token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QifQ.eyJhdWQiOiJ0ZXN0X2NsdXN0ZXIiLCJleHAiOjI2NjEyNjM5NTcsImlhdCI6MTY2MTI2Mzk1NywiaXNzIjoiaXNzdWVyIiwic3ViIjoidGVzdCJ9.Z0Hyi7YbnRZRfOJxjz0K9b1bFNA4eoWa4g8kH5LoYRivvARAZLdD7Ux0OQfsrFHAjK4eOtglF4nmY0usGl8diUsL86ifinyxMNC78xzaKrV620Kzt2k2kld0cwCPc-pRAjN8RSMw6Ypt9oIpnFTsFwIhB9QN_7t6KF4NRjgqdENI4UbBTgw0cR5kExk7PGpyEIxJ_6Y0cVwCBgosnKAEA7XpA2fHU_k61zX9MIiDgdnwWl0KuB3Csr37N998T-oxQPNI8o9JVwsSYGPPVvET70PankDUNhVWrU7rxKVVQ579khhdApPpDB82lypI7W8eVcZoamTWo19o1_CMUSzb2A"
var jwk2Token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QyIn0.eyJhdWQiOiJ0ZXN0X2NsdXN0ZXIiLCJleHAiOjI2NjEyNjQyNjksImlhdCI6MTY2MTI2NDI2OSwiaXNzIjoiaXNzdWVyMiIsInN1YiI6InRlc3QyIn0.Tot41E-wSz24wo1wj3b8CwEr-O_dqWZoHZkAh2x4nfK2hT4yhfiOcajmKQJVVZX2_897c8uDOqfLzl77JEe-AX4mlEBZXWUNqwwQIdIFZxpL6FEV_YjvTF0bQuu9oeD7kYW-6i3-QQpB6QpCVb-wLW8bBbJ4zCap88nYk14HZH-ZYSzPAP7YEVppHQNhWrxQ66nQU__RuYeQdL6J5Edes9qCHUgqnZCnMPzDZ4l_3Pc5tTSNVcOUl5MMHsvrYsb0VtSFTNCOjJIADXbc2KzVbfqLt-ArUDxs36__u_g84TfGFXoT0VTDbDjYwD7wpyLuT3oLcJuA4m_tto6Rrn7Rww"

var expiredJwk2Token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QyIn0.eyJhdWQiOiJ0ZXN0X2NsdXN0ZXIiLCJleHAiOjE2NjEyNjQzOTgsImlhdCI6MTY2MTI2NDM5OCwiaXNzIjoiaXNzdWVyMiIsInN1YiI6InRlc3QyIn0.1nWuqpwj4uPDk0pyyqEJhpIgyridv699B7OjEBGSyQ8iyrqryeG1yr7oP1qnKlrcqtbVmuB5ELJoXNUerd8BL0GQBMCkkxjG1cuLvLNOWo5yzifcfYHiiaCL25EblWG46eBrxAeHmqGigQiIpSUPjQTlZT_lRLrEI9h_xQhwNp5AnsY2S1f8N4oaMqjUjgREGdLhZT9sOyNmrf5uowTFcR3aWBkpIB5Ac5rvI8-U7-D1rY5KJ3Wez4G2L3Miyof_lOlK1g8XwAasCPKlhHea5qZNjqHLqgOb5EIQ_yd_KICT7pFLSgMXw_IJ9c68z-H1N7wEivnnLydgQUR3WVEytA"

func TestMain(m *testing.M) {
	defer utilccl.TestingEnableEnterprise()()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

func TestJWTEnabledCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// JWT auth is not enabled.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(""))
	require.ErrorContains(t, err, "JWT authentication: not enabled")

	// Enable JWT auth.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	// Now the validate call gets past the enabled check and fails on the next check (token validity).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(""))
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

	// When no JWKS is specified the token will be invalid.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Set the JWKS cluster setting.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, jwk1)

	// Now the validate call gets past the token validity check and fails on the next check (subject matching user)
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
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
	// Set the JWKS to only include jwk1.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+"]}")

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens signed with a different key.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Set both jwk1 and jwk2 to be valid signing keys.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+", "+jwk2+"]}")

	// Now jwk2 token passes the validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestExpiredToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens with an expiration date in the past.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(expiredJwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")
}

func TestKeyIdMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	// Set JWKS field to contain JWK1 and JWK2 but have an incorrect key id for jwk1.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1WithWrongKid+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an invalid token error for tokens with a kid not equal to that in JWKS.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	// Set JWKS field to contain JWK1 and JWK2 with the normal key ids for both.
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	// Now jwk1 token passes the validity check and fails on the next check (subject matching user).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestSubjectCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with a subject error when a user tries to log in with a user named
	// "invalid" but the token is for the user "test2".
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")

	// Validation passes the subject check when the username matches the subject and then fails on the next
	// check (audience field not matching).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
}

func TestAudienceCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled and accepts jwk1 or jwk2 as valid signing keys.
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the audience in the token doesn't match the cluster's audience.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid audience")

	// Update the audience field to "test_cluster".
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "test_cluster")

	// Validation passess the issuer check now that they match and fails on the next check (issuer field not matching).
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
}

func TestIssuerCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	// Make sure jwt auth is enabled, accepts jwk1 or jwk2 as valid signing keys, accepts the audience of "test_cluster"
	// and the issuer of "issuer2".
	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthJWKS.Override(ctx, &s.ClusterSettings().SV, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	JWTAuthAudience.Override(ctx, &s.ClusterSettings().SV, "test_cluster")
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, "issuer2")

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Validation fails with an audience error when the issuer in the token is equal to the cluster's accepted issuers.
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")

	// Validation succeeds when the issuer in the token is equal to the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.NoError(t, err)

	// Set the cluster setting to accept issuer values of either "issuer" or "issuer2".
	JWTAuthIssuers.Override(ctx, &s.ClusterSettings().SV, "[\"issuer\", \"issuer2\"]")

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test"), []byte(jwk1Token))
	require.NoError(t, err)

	// Validation succeeds when the issuer in the token is an element of the cluster's accepted issuers.
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.NoError(t, err)

}
