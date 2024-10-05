// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgurl

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestURL(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "url"), func(t *testing.T, td *datadriven.TestData) string {
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

func TestClone(t *testing.T) {
	u := New()
	require.NoError(t, u.SetOption("user", "testuser"))
	require.Equal(t, u.GetUsername(), "testuser")
	require.NoError(t, u.SetOption("application_name", "testapp"))
	require.Equal(t, []string{"testapp"}, u.extraOptions["application_name"])

	u2 := u.Clone()
	// u2 has initial values from u.
	require.Equal(t, u2.GetUsername(), "testuser")
	require.Equal(t, []string{"testapp"}, u2.extraOptions["application_name"])

	// Modifications to u2 only impact u2.
	require.NoError(t, u2.SetOption("user", "testuser2"))
	require.NoError(t, u2.SetOption("application_name", "testapp2"))
	require.Equal(t, u2.GetUsername(), "testuser2")
	require.Equal(t, u.GetUsername(), "testuser")
	require.Equal(t, []string{"testapp"}, u.extraOptions["application_name"])
	require.Equal(t, []string{"testapp2"}, u2.extraOptions["application_name"])
}

// Silence the unused linter
var _ = ProtoUndefined
var _ = TLSVerifyCA
var _ = TLSPrefer
var _ = TLSAllow

func TestParseExtendedOptions(t *testing.T) {
	u, err := Parse("postgres://localhost?options= " +
		"--user=test " +
		"-c    search_path=public,testsp %20%09 " +
		"--default-transaction-isolation=read\\ uncommitted   " +
		"-capplication_name=test  " +
		"--DateStyle=ymd\\ ,\\ iso\\  " +
		"-c intervalstyle%3DISO_8601 " +
		"-ccustom_option.custom_option=test2")
	require.NoError(t, err)
	opts := u.GetOption("options")
	kvs, err := ParseExtendedOptions(opts)
	require.NoError(t, err)
	require.Equal(t, kvs.Get("user"), "test")
	require.Equal(t, kvs.Get("search_path"), "public,testsp")
	require.Equal(t, kvs.Get("default_transaction_isolation"), "read uncommitted")
	require.Equal(t, kvs.Get("application_name"), "test")
	require.Equal(t, kvs.Get("DateStyle"), "ymd , iso ")
	require.Equal(t, kvs.Get("intervalstyle"), "ISO_8601")
	require.Equal(t, kvs.Get("custom_option.custom_option"), "test2")
}

func TestEncodeExtendedOptions(t *testing.T) {
	kvs := url.Values{}
	kvs.Set("user", "test")
	kvs.Set("a b", "test")
	kvs.Set("test", "c d")
	kvs.Set(`a\ b`, `c\d`)

	opts := EncodeExtendedOptions(kvs)
	require.Equal(t, `-ca\ b=test -ca\\\ b=c\\d -ctest=c\ d -cuser=test `, opts)

	kv2, err := ParseExtendedOptions(opts)
	require.NoError(t, err)
	require.Equal(t, kvs, kv2)
}
