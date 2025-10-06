// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestValidateAndParseJWTAuthIssuers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name                  string
		setting               string
		wantErr               bool
		expectedIssuers       []string
		expectedIssuerJWKSMap map[string]string
	}{
		{name: "empty string",
			expectedIssuers: []string{""}},
		{name: "string constant",
			setting:         "issuer1",
			expectedIssuers: []string{"issuer1"}},
		{name: "odd string constant",
			setting:         "issuer1{}`[]!@#%#^$&*",
			expectedIssuers: []string{"issuer1{}`[]!@#%#^$&*"}},
		{name: "empty json",
			setting:         "[]",
			expectedIssuers: []string{}},
		{name: "single element json",
			setting:         "[\"issuer 1\"]",
			expectedIssuers: []string{"issuer 1"}},
		{name: "multiple element json",
			setting:         "[\"issuer 1\", \"issuer 2\", \"issuer 3\", \"issuer 4\", \"issuer 5\"]",
			expectedIssuers: []string{"issuer 1", "issuer 2", "issuer 3", "issuer 4", "issuer 5"}},
		{name: "json but invalid in this context",
			setting: "{\"redirect_urls\": {\"key\":\"http://example.com\"}}", wantErr: true},
		{name: "json object valid in this context single mapping",
			setting:               "{\"issuer_jwks_map\": {\"https://accounts.google.com\": \"https://www.googleapis.com/oauth2/v3/certs\"}}",
			expectedIssuers:       []string{"https://accounts.google.com"},
			expectedIssuerJWKSMap: map[string]string{"https://accounts.google.com": "https://www.googleapis.com/oauth2/v3/certs"}},
		{name: "json object valid in this context multiple mapping",
			setting: "{\"issuer_jwks_map\": {\"https://accounts.google.com\": \"https://www.googleapis.com/oauth2/v3/certs\"" +
				",\"example.com/adfs\": \"https://example.com/adfs/discovery/keys\"}}",
			expectedIssuers: []string{"https://accounts.google.com", "example.com/adfs"},
			expectedIssuerJWKSMap: map[string]string{"https://accounts.google.com": "https://www.googleapis.com/oauth2/v3/certs",
				"example.com/adfs": "https://example.com/adfs/discovery/keys"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateJWTAuthIssuersConf(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateJWTAuthIssuers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				parsedIssuersConf := mustParseJWTIssuersConf(tt.setting)
				require.ElementsMatch(t, parsedIssuersConf.issuers, tt.expectedIssuers)
				if parsedIssuersConf.ijMap != nil {
					require.Equal(t, parsedIssuersConf.ijMap.Mappings, tt.expectedIssuerJWKSMap)
				}
			}
		})
	}
}

func TestValidateAndParseJWTAuthAudience(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name            string
		setting         string
		wantErr         bool
		expectedIssuers []string
	}{
		{"empty string",
			"", false,
			[]string{""}},
		{"string constant",
			"audience1", false,
			[]string{"audience1"}},
		{"odd string constant",
			"audience1{}`[]!@#%#^$&*", false,
			[]string{"audience1{}`[]!@#%#^$&*"}},
		{"empty json",
			"[]", false,
			[]string{}},
		{"single element json",
			"[\"audience 1\"]", false,
			[]string{"audience 1"}},
		{"multiple element json",
			"[\"audience 1\", \"audience 2\", \"audience 3\", \"audience 4\", \"audience 5\"]", false,
			[]string{"audience 1", "audience 2", "audience 3", "audience 4", "audience 5"}},
		{"json but invalid in this context",
			"{\"redirect_urls\": {\"key\":\"http://example.com\"}}", true,
			nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateJWTAuthAudiences(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateJWTAuthAudiences() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				parsedAudiences := mustParseValueOrArray(tt.setting)
				require.Equal(t, parsedAudiences, tt.expectedIssuers)
			}
		})
	}
}

func TestValidateAndParseJWTAuthJWKS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name          string
		setting       string
		wantErr       bool
		length        int
		containedKeys []string
	}{
		{"empty string",
			"", true,
			0, nil},
		{"string constant",
			"issuer1", true,
			0, nil},
		{"empty json",
			"{}", true,
			0, nil},
		{"proper formatted empty JWKS",
			"{\"keys\": []}", false,
			0, []string{}},
		{"standalone JWK",
			"{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test\", \"n\": \"sJCwOk5gVjZZu3oaODecZaT_-Lee7J-q3rQIvCilg-7B8fFNJ2XHZCsF74JX2d7ePyjz7u9d2r5CvstufiH0qGPHBBm0aKrxGRILRGUTfqBs8Dnrnv9ymTEFsRUQjgy9ACUfwcgLVQIwv1NozySLb4Z5N8X91b0TmcJun6yKjBrnr1ynUsI_XXjzLnDpJ2Ng_shuj-z7DKSEeiFUg9eSFuTeg_wuHtnnhw4Y9pwT47c-XBYnqtGYMADSVEzKLQbUini0p4-tfYboF6INluKQsO5b1AZaaXgmStPIqteS7r2eR3LFL-XB7rnZOR4cAla773Cq5DD-8RnYamnmmLu_gQ\", \"e\": \"AQAB\"}",
			false,
			1, []string{"test"}},
		{"valid single key in JWKS format",
			"{\"keys\":[{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test\", \"n\": \"sJCwOk5gVjZZu3oaODecZaT_-Lee7J-q3rQIvCilg-7B8fFNJ2XHZCsF74JX2d7ePyjz7u9d2r5CvstufiH0qGPHBBm0aKrxGRILRGUTfqBs8Dnrnv9ymTEFsRUQjgy9ACUfwcgLVQIwv1NozySLb4Z5N8X91b0TmcJun6yKjBrnr1ynUsI_XXjzLnDpJ2Ng_shuj-z7DKSEeiFUg9eSFuTeg_wuHtnnhw4Y9pwT47c-XBYnqtGYMADSVEzKLQbUini0p4-tfYboF6INluKQsO5b1AZaaXgmStPIqteS7r2eR3LFL-XB7rnZOR4cAla773Cq5DD-8RnYamnmmLu_gQ\", \"e\": \"AQAB\"}]}",
			false,
			1, []string{"test"}},
		{"invalid standalone key",
			"{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test\"}", true,
			0, nil},
		{"valid multiple keys in JWKS",
			"{\"keys\":[{\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test\", \"n\": \"sJCwOk5gVjZZu3oaODecZaT_-Lee7J-q3rQIvCilg-7B8fFNJ2XHZCsF74JX2d7ePyjz7u9d2r5CvstufiH0qGPHBBm0aKrxGRILRGUTfqBs8Dnrnv9ymTEFsRUQjgy9ACUfwcgLVQIwv1NozySLb4Z5N8X91b0TmcJun6yKjBrnr1ynUsI_XXjzLnDpJ2Ng_shuj-z7DKSEeiFUg9eSFuTeg_wuHtnnhw4Y9pwT47c-XBYnqtGYMADSVEzKLQbUini0p4-tfYboF6INluKQsO5b1AZaaXgmStPIqteS7r2eR3LFL-XB7rnZOR4cAla773Cq5DD-8RnYamnmmLu_gQ\", \"e\": \"AQAB\"}, {\"kty\": \"RSA\", \"use\": \"sig\", \"alg\": \"RS256\", \"kid\": \"test2\", \"n\": \"3gOrVdePypBAs6bTwD-6dZhMuwOSq8QllMihBfcsiRmo3c14_wfa_DRDy3kSsacwdih5-CaeF8ou-Dan6WqXzjDyJNekmGltPLfO2XB5FkHQoZ-X9lnXktsAgNLj3WsKjr-xUxrh8p8FFz62HJYN8QGaNttWBJZb3CgdzF7i8bPqVet4P1ekzs7mPBH2arEDy1f1q4o7fpmw0t9wuCrmtkj_g_eS6Hi2Rxm3m7HJUFVVbQeuZlT_W84FUzpSQCkNi2QDvoNVVCE2DSYZxDrzRxSZSv_fIh5XeJhwYY-f8iEfI4qx91ONGzGMvPn2GagrBnLBQRx-6RsORh4YmOOeeQ\", \"e\": \"AQAB\"}]}",
			false,
			2, []string{"test", "test2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateJWTAuthJWKS(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateJWTAuthIssuers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				parsedJWKS := mustParseJWKS(tt.setting)
				require.Equal(t, parsedJWKS.Len(), tt.length)
				for _, key := range tt.containedKeys {
					_, ok := parsedJWKS.LookupKeyID(key)
					require.True(t, ok)
				}
			}
		})
	}
}
