// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package poison_test

import (
	"context"
	"path/filepath"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // to init roachpb.PrettyPrintRange
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestPoisonedError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	err := errors.DecodeError(ctx, errors.EncodeError(ctx, poison.NewPoisonedError(
		roachpb.Span{Key: roachpb.Key("a")}, hlc.Timestamp{WallTime: 1},
	)))
	require.True(t, errors.HasType(err, (*poison.PoisonedError)(nil)), "%+v", err)
	var buf redact.StringBuilder
	buf.Printf("%s", err)
	echotest.Require(t, string(buf.RedactableString()), filepath.Join("testdata", "poisoned_error.txt"))
}
