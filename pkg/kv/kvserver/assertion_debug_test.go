// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGobDecodeReplicatedCmd(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(
		os.ExpandEnv("$HOME"), "go", "src", "github.com", "cockroachdb", "cockroach", "assertion", "replicatedCmd999518795",
	))
	require.NoError(t, err)
	dec := gob.NewDecoder(bytes.NewReader(b))
	cmd := &replicatedCmd{}
	require.NoError(t, dec.DecodeValue(reflect.ValueOf(cmd)))
}
