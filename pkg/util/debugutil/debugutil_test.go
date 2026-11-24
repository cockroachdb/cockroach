// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debugutil

import (
	"testing"

	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestStack(t *testing.T) {
	var buf redact.StringBuilder
	buf.Print(Stack())
	buf.Printf("%s", Stack())
	buf.Printf("%v", Stack())
	out := buf.RedactableString()

	require.NotContains(t, out, "‹")
	require.NotContains(t, out, "›")
}
