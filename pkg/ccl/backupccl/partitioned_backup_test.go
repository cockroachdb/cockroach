// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestGetURIsByLocalityKV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type TestCase struct {
		name           string
		input          []string
		defaultURI     string
		urisByLocality map[string]string
		error          string
	}

	testCases := []TestCase{
		{
			name: "singleURINoDefault",
			input: []string{
				"s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			},
			defaultURI:     "s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			urisByLocality: map[string]string{},
		},
		{
			name: "singleURIDefault",
			input: []string{
				"s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=default",
			},
			defaultURI:     "s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			urisByLocality: map[string]string{},
		},
		{
			name: "multipleURIs",
			input: []string{
				"s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=default",
				"s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc1",
			},
			defaultURI: "s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			urisByLocality: map[string]string{
				"dc=dc1": "s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			},
		},
		{
			name: "multipleURISameStore",
			input: []string{
				"s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=default",
				"s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc1",
				"s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc2",
			},
			defaultURI: "s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			urisByLocality: map[string]string{
				"dc=dc1": "s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
				"dc=dc2": "s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			},
		},
		{
			name: "singleURILocality",
			input: []string{
				"s3://foo?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc1",
			},
			error: "COCKROACH_LOCALITY dc=dc1 is invalid for a single BACKUP location",
		},
		{
			name: "noDefault",
			input: []string{
				"s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc1",
				"s3://baz?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc2",
			},
			error: "no default URL provided for partitioned backup",
		},
		{
			name: "duplicateLocality",
			input: []string{
				"s3://bar?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc1",
				"s3://baz?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&COCKROACH_LOCALITY=dc%3Ddc1",
			},
			error: "duplicate URIs for locality dc=dc1",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defaultURI, urisByLocality, err := getURIsByLocalityKV(tc.input, "" /* appendPath */)
			if tc.error != "" {
				if !testutils.IsError(err, tc.error) {
					t.Fatalf("expected error matching %q, got %q", tc.error, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if defaultURI != tc.defaultURI {
					t.Fatalf("expected default URI %s, got %s", tc.defaultURI, defaultURI)
				}
				for k, v := range urisByLocality {
					if ev, ok := tc.urisByLocality[k]; !ok {
						t.Fatalf("expected key %s not found", k)
					} else if ev != v {
						t.Fatalf("expected value %s for key %s, found %s", v, k, ev)
					}
				}
				for k := range tc.urisByLocality {
					if _, ok := tc.urisByLocality[k]; !ok {
						t.Fatalf("unexpected key %s", k)
					}
				}
			}
		})
	}
}
