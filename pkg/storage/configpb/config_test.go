// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package configpb

import (
	"os"
	"path/filepath"
	"testing"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMarshallUnmarshall(t *testing.T) {
	expected := Storage{
		Stores: []Store{
			{
				Path: "foo",
				// NB: nil vs empty arrays are not handled correctly by the json
				// encoder. See https://github.com/golang/protobuf/issues/1348.
				// This shouldn't cause any problems in practice, but after
				// encoding and decoding a nil field becomes a non-nil
				// attribute.
				Attributes: roachpb.Attributes{Attrs: []string{}},
			},
		},
	}

	b, err := expected.ToJson(true)
	require.NoError(t, err)
	actual, err := FromJson(b)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestStoreSpecListPreventedStartupMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	boomStoreDir := filepath.Join(dir, "boom")
	boomAuxDir := filepath.Join(boomStoreDir, AuxiliaryDir)
	okStoreDir := filepath.Join(dir, "ok")
	okAuxDir := filepath.Join(okStoreDir, AuxiliaryDir)

	for _, sd := range []string{boomAuxDir, okAuxDir} {
		require.NoError(t, os.MkdirAll(sd, 0755))
	}

	ssl := Storage{
		Stores: []Store{
			{InMemory: true},
			{Path: okStoreDir},
			{Path: boomStoreDir},
		},
	}

	err := ssl.PriorCriticalAlertError()
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(ssl.Stores[2].PreventedStartupFile(), []byte("boom"), 0644))

	err = ssl.PriorCriticalAlertError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "startup forbidden by prior critical alert")
	require.Contains(t, errors.FlattenDetails(err), "boom")
}
