// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/stretchr/testify/assert"
)

func TestTypeString(t *testing.T) {
	for i := 0; i < jobspb.NumJobTypes; i++ {
		typ := jobspb.Type(i)
		typStr := typ.String()
		convertedType, err := jobspb.TypeFromString(typStr)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, convertedType, typ)
	}
}
