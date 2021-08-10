// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgurl

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestURL(t *testing.T) {
	datadriven.RunTest(t, "testdata/url", func(t *testing.T, td *datadriven.TestData) string {
		var result bytes.Buffer

		var u *URL
		switch td.Cmd {
		case "insecure":
			u = New().WithInsecure()

		case "url":
			var err error
			u, err = Parse(td.Input)
			if err != nil {
				fmt.Fprintf(&result, "parse error: %v", err)
				return result.String()
			}

		default:
			t.Fatalf("unrecognized command: %s", td.Cmd)
		}

		if err := u.Validate(); err != nil {
			fmt.Fprintf(&result, "%v\nDetails:\n%s----\n", err, errors.FlattenDetails(err))
		}

		fmt.Fprintf(&result, "pq URL: %s\n", u.ToPQ())
		fmt.Fprintf(&result, "DSN:    %s\n", u.ToDSN())
		fmt.Fprintf(&result, "JDBC:   %s\n", u.ToJDBC())

		u.
			WithDefaultUsername("defaultuser").
			WithDefaultDatabase("defaultdb").
			WithDefaultHost("defaulthost").
			WithDefaultPort("26257")
		fmt.Fprintln(&result, "--defaults filled--")
		fmt.Fprintf(&result, "pq URL: %s\n", u.ToPQ())
		fmt.Fprintf(&result, "DSN:    %s\n", u.ToDSN())
		fmt.Fprintf(&result, "JDBC:   %s\n", u.ToJDBC())

		return result.String()
	})
}

func TestPassword(t *testing.T) {
	u := New()

	enabled, _, _ := u.GetAuthnPassword()
	require.False(t, enabled)

	u.WithAuthn(AuthnPassword(true, "abc"))

	enabled, hasp, p := u.GetAuthnPassword()
	require.True(t, enabled)
	require.True(t, hasp)
	require.Equal(t, p, "abc")

	u.ClearPassword()
	enabled, hasp, _ = u.GetAuthnPassword()
	require.True(t, enabled)
	require.False(t, hasp)
}

func TestCopyAuthn(t *testing.T) {
	u := New()
	v := New()

	u.WithAuthn(AuthnPassword(true, "abc"))
	opt, err := u.GetAuthnOption()
	require.NoError(t, err)
	v.WithAuthn(opt)
	enabled, hasp, p := v.GetAuthnPassword()
	require.True(t, enabled)
	require.True(t, hasp)
	require.Equal(t, p, "abc")

	u.WithAuthn(AuthnClientCert("a", "b"))
	opt, err = u.GetAuthnOption()
	require.NoError(t, err)
	v.WithAuthn(opt)

	enabled, patha, pathb := v.GetAuthnCert()
	require.True(t, enabled)
	require.Equal(t, patha, "a")
	require.Equal(t, pathb, "b")
}

func TestOptions(t *testing.T) {
	u := New()

	// Check that AddOptions processes the options as per Parse().
	err := u.AddOptions(url.Values{"user": []string{"foo"}, "database": []string{"bar"}})
	require.NoError(t, err)
	require.Equal(t, u.GetUsername(), "foo")
	require.Equal(t, u.GetDatabase(), "bar")
	_, ok := u.extraOptions["user"]
	require.Equal(t, ok, false)
	_, ok = u.extraOptions["database"]
	require.Equal(t, ok, false)

	// Check that non-special options remain in extraOptions.
	err = u.AddOptions(url.Values{"application_name": []string{"foo", "bar"}})
	require.NoError(t, err)
	require.Equal(t, u.extraOptions["application_name"], []string{"foo", "bar"})

	// Check that SetOption stores just one value.
	err = u.SetOption("user", "bar")
	require.NoError(t, err)
	require.Equal(t, u.GetUsername(), "bar")
	_, ok = u.extraOptions["user"]
	require.Equal(t, ok, false)

	// Check that non-special options remain in extraOptions.
	err = u.SetOption("application_name", "baz")
	require.NoError(t, err)
	require.Equal(t, u.extraOptions["application_name"], []string{"baz"})
}

// Silence the unused linter
var _ = ProtoUndefined
var _ = TLSVerifyCA
var _ = TLSPrefer
var _ = TLSAllow
