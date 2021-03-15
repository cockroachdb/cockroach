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
	"github.com/stretchr/testify/require"
)

func TestViperDenyList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgFile, err := ioutil.TempFile("", "*_denylist.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(cfgFile.Name()) }()

	dl, err := NewViperDenyListFromFile(ctx, cfgFile.Name(), time.Millisecond)
	require.NoError(t, err)

	e, err := dl.Denied("123")
	require.NoError(t, err)
	require.True(t, e == nil)

	_, err = cfgFile.Write([]byte("456: denied"))
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	e, err = dl.Denied("456")
	require.NoError(t, err)
	require.Equal(t, &Entry{Reason: "denied"}, e)
}
