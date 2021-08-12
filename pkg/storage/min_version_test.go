// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestMinVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	version1 := &roachpb.Version{Major: 21, Minor: 1, Patch: 0, Internal: 122}
	version2 := &roachpb.Version{Major: 21, Minor: 1, Patch: 0, Internal: 126}

	mem := vfs.NewMem()
	dir := "/foo"
	require.NoError(t, mem.MkdirAll(dir, os.ModeDir))

	// Expect nil version when min version file doesn't exist.
	v, err := GetMinVersion(mem, dir)
	require.NoError(t, err)
	require.Nil(t, v)

	// Expect min version to not be at least any target version.
	ok, err := MinVersionIsAtLeastTargetVersion(mem, dir, version1)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version2)
	require.NoError(t, err)
	require.False(t, ok)

	// Expect no error when updating min version if no file currently exists.
	v = &roachpb.Version{}
	proto.Merge(v, version1)
	require.NoError(t, WriteMinVersionFile(mem, dir, v))

	// Expect min version to be version1.
	v, err = GetMinVersion(mem, dir)
	require.NoError(t, err)
	require.True(t, version1.Equal(v))

	// Expect min version to be at least version1 but not version2.
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version2)
	require.NoError(t, err)
	require.False(t, ok)

	// Expect no error when updating min version to a higher version.
	v = &roachpb.Version{}
	proto.Merge(v, version2)
	require.NoError(t, WriteMinVersionFile(mem, dir, v))

	// Expect min version to be at least version1 and version2.
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version2)
	require.NoError(t, err)
	require.True(t, ok)

	// Expect min version to be version2.
	v, err = GetMinVersion(mem, dir)
	require.NoError(t, err)
	require.True(t, version2.Equal(v))

	// Expect no-op when trying to update min version to a lower version.
	v = &roachpb.Version{}
	proto.Merge(v, version1)
	require.NoError(t, WriteMinVersionFile(mem, dir, v))
	v, err = GetMinVersion(mem, dir)
	require.NoError(t, err)
	require.True(t, version2.Equal(v))
}
