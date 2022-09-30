// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nullsink

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestNullSinkReadAndWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dest := MakeNullSinkStorageURI("foo")

	conf, err := cloud.ExternalStorageConfFromURI(dest, username.RootUserName())
	if err != nil {
		t.Fatal(err)
	}

	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{},
		nil, /* Cluster Settings */
		nil, /* blobClientFactory */
		nil, /* ie */
		nil, /* ief */
		nil, /* kvDB */
		nil, /* limiters */
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	require.Equal(t, cloudpb.ExternalStorage{Provider: cloudpb.ExternalStorageProvider_null}, s.Conf())
	require.NoError(t, cloud.WriteFile(ctx, s, "", bytes.NewReader([]byte("abc"))))
	sz, err := s.Size(ctx, "")
	require.NoError(t, err)
	require.Equal(t, int64(0), sz)
	_, err = s.ReadFile(ctx, "")
	require.True(t, errors.Is(err, io.EOF))
}
