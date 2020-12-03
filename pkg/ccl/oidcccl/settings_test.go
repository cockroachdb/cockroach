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
		name         string
		setting      string
		wantErr      bool
		expectParsed *multiRegionRedirectURLs
	}{
		{"empty string",
			"", false,
			&multiRegionRedirectURLs{nil, ""}},
		{"url string",
			"https://some.long.example.com/a/b/c/d/#", false,
			&multiRegionRedirectURLs{nil, "https://some.long.example.com/a/b/c/d/#"}},
		{"json missing default url",
			"{\"wrong\":\"key\"}", true, nil},
		{"json with default url only",
			"{\"default_url\":\"example.com\"}", false,
			&multiRegionRedirectURLs{nil, "example.com"}},
		{"json with default url and extra nonsense",
			"{\"default_url\":\"example.com\",\"wrong\":\"key\"}", false,
			&multiRegionRedirectURLs{nil, "example.com"}},
		{"json with default url and redirect urls wrong type",
			"{\"default_url\":\"example.com\",\"redirect_urls\":\"key\"}", true, nil},
		{"json with default url and redirect urls correct type",
			"{\"default_url\":\"example.com\",\"redirect_urls\": {\"key\":\"value\"}}", false,
			&multiRegionRedirectURLs{map[string]string{"key": "value"}, "example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateOIDCRedirectURL(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateOIDCRedirectURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				// If we don't expect an error we *must* expect a non-nil object parsed
				require.NotNil(t, tt.expectParsed)
				parsedMultiRegionRedirectURLs := mustParseOIDCRedirectURL(tt.setting)
				require.Equal(t, &parsedMultiRegionRedirectURLs, tt.expectParsed)
			}
		})
	}
}
