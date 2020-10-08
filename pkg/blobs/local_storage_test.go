// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blobs

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirectoryNormalization(t *testing.T) {
	l, err := NewLocalStorage("././.")
	if err != nil {
		t.Fatal(err)
	}
	expected, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expected, l.externalIODir)
}
