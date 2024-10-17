// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientName(t *testing.T) {
	options := func(options ...ExternalStorageOption) ExternalStorageOptions {
		context := EarlyBootExternalStorageContext{
			Options: options,
		}
		return context.ExternalStorageOptions()
	}
	require.Empty(t, options().ClientName)
	require.Equal(t, options(WithClientName("this-is-the-name")).ClientName, "this-is-the-name")
}
