// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestViperForwardCompatibility makes sure that the new config file format
// will not return error when ingested by the old binary, and will not
// cause sqlproxy fail to start.
func TestViperForwardCompatibility(t *testing.T) {
	// Make sure that new config file format will not
	// cause errors for the old binary.
	defer leaktest.AfterTest(t)()

	files := []File{
		{
			Seq: 3,
			Denylist: []*DenyEntry{
				{DenyEntity{"1.1.1.1", IPAddrType}, timeutil.Now().Add(time.Hour), "some reason"},
				{DenyEntity{"63", ClusterType}, timeutil.Now().Add(2 * time.Hour), "another reason"},
			},
		},
		{
			// empty file
		},
		{
			Seq: 7,
			// empty list
		},
	}
	for _, file := range files {
		cfgFile, err := ioutil.TempFile("", "*_denylist.yml")
		require.NoError(t, err)
		defer func() { _ = os.Remove(cfgFile.Name()) }()

		raw, err := file.Serialize()
		require.NoError(t, err)
		err = ioutil.WriteFile(cfgFile.Name(), raw, 0777)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// make sure old parser won't break on new config format
		_, err = NewViperDenyListFromFile(ctx, cfgFile.Name(),
			time.Minute)
		require.NoError(t, err)
	}
}

func TestViperDenyList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgFile, err := ioutil.TempFile("", "*_denylist.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(cfgFile.Name()) }()

	dl, err := NewViperDenyListFromFile(ctx, cfgFile.Name(), time.Millisecond)
	require.NoError(t, err)

	e, err := dl.Denied(DenyEntity{"123", ClusterType})
	require.NoError(t, err)
	require.True(t, e == nil)

	_, err = cfgFile.Write([]byte("456: denied"))
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	e, err = dl.Denied(DenyEntity{"456", ClusterType})
	require.NoError(t, err)
	require.Equal(t, &Entry{Reason: "denied"}, e)
}
