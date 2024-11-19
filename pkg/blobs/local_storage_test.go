// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
