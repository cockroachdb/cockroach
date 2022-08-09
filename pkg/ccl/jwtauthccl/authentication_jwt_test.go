package jwtauthccl

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// JWT not enabled
	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(""))
	require.ErrorContains(t, err, "JWT authentication: not enabled")

	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(""))
	require.ErrorContains(t, err, "JWT authentication: invalid token")
}

func TestJWTSingleKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, jwk1)
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestJWTMultiKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestExpiredToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(expiredJwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")
}

func TestKeyIdMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1WithWrongKid+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid token")

	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")
}

func TestSubjectCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("invalid"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid subject")

	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid audience")
}

func TestAudienceCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid audience")

	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.audience = 'test_cluster'`)
	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")
}

func TestIssuerCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.jwks = $1`, "{\"keys\":["+jwk1+", "+jwk2+"]}")
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.audience = 'test_cluster'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.issuers = 'issuer2'`)

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	err := verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test"), []byte(jwk1Token))
	require.ErrorContains(t, err, "JWT authentication: invalid issuer")

	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.NoError(t, err)

	sqlDB.Exec(t, `SET CLUSTER SETTING server.jwt_authentication.issuers = '["issuer", "issuer2"]'`)

	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test"), []byte(jwk1Token))
	require.NoError(t, err)

	err = verifier.ValidateJWTLogin(s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("test2"), []byte(jwk2Token))
	require.NoError(t, err)

}
