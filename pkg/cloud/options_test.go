// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientName(t *testing.T) {
	options := func(options ...ExternalStorageOption) ExternalStorageOptions {
		context := ExternalStorageContext{
			Options: options,
		}
		return context.ExternalStorageOptions()
	}
	require.Empty(t, options().ClientName)
	require.Equal(t, options(WithClientName("this-is-the-name")).ClientName, "this-is-the-name")
}
