// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupdest_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuputils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestJoinURLPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// path.Join has identical behavior for these inputs.
	require.Equal(t, "/top/path", backuputils.JoinURLPath("/top", "path"))
	require.Equal(t, "top/path", backuputils.JoinURLPath("top", "path"))

	require.Equal(t, "/path", backuputils.JoinURLPath("/top", "../path"))
	require.Equal(t, "path", backuputils.JoinURLPath("top", "../path"))

	require.Equal(t, "../path", backuputils.JoinURLPath("top", "../../path"))

	// path.Join has different behavior for this input.
	require.Equal(t, "/../path", backuputils.JoinURLPath("/top", "../../path"))

}
