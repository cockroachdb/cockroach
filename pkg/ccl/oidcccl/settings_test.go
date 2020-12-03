// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_validateAndParseOIDCRedirectURL(t *testing.T) {
	tests := []struct {
		name              string
		setting           string
		wantErr           bool
		expectMultiRegion *multiRegionRedirectURLs
		expectSingleURL   *singleRedirectURL
	}{
		{"empty string",
			"", false,
			nil, &singleRedirectURL{RedirectURL: ""}},
		{"URL string",
			"https://some.long.example.com/a/b/c/d/#", false,
			nil,
			&singleRedirectURL{RedirectURL: "https://some.long.example.com/a/b/c/d/#"}},
		{"json contains unexpected keys",
			"{\"wrong\":\"key\"}", true,
			nil, nil},
		{"json with extra nonsense",
			"{\"redirect_urls\": {\"key\":\"example.com\"},\"wrong\":\"key\"}", true,
			nil, nil},
		{"json with redirect URLs wrong type",
			"{\"redirect_urls\":\"key\"}", true,
			nil, nil},
		{"json with invalid redirect URL",
			"{\"redirect_urls\":{\"key\":\"{}{}{}http://example.com\"}}", true,
			nil, nil},
		{"json with redirect URLs correct type",
			"{\"redirect_urls\": {\"key\":\"http://example.com\"}}", false,
			&multiRegionRedirectURLs{
				map[string]string{"key": "http://example.com"},
			},
			nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateOIDCRedirectURL(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateOIDCRedirectURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				parsedRedirect := mustParseOIDCRedirectURL(tt.setting)

				if tt.expectMultiRegion != nil {
					require.Equal(t, parsedRedirect.mrru, tt.expectMultiRegion)

				}
				if tt.expectSingleURL != nil {
					require.Equal(t, parsedRedirect.sru, tt.expectSingleURL)
				}
			}
		})
	}
}
