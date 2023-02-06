// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
