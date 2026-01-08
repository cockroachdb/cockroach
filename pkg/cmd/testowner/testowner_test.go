package main

// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseQuery(t *testing.T) {
	pkg, test := parseQuery("github.com/cockroachdb/cockroach/pkg/build.TestFoo")
	require.Equal(t, "github.com/cockroachdb/cockroach/pkg/build", pkg)
	require.Equal(t, "TestFoo", test)
	pkg, test = parseQuery("pkg/build.TestFoo")
	require.Equal(t, "pkg/build", pkg)
	require.Equal(t, "TestFoo", test)
}
