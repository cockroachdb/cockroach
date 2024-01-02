// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnostics

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/mitchellh/reflectwalk"
	"github.com/stretchr/testify/require"
)

// TestStringRedactor_Primitive tests that fields of type `*string` will be
// correctly redacted to "_", and all other field types ( including `string`)
// will be unchanged.
func TestStringRedactor_Primitive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type Foo struct {
		A string
		B *string
		C map[string]string
	}

	string1 := "string 1"
	string2 := "string 2"
	foo := Foo{
		A: string1,
		B: &string2,
		C: map[string]string{"3": "string 3"},
	}

	require.NoError(t, reflectwalk.Walk(foo, stringRedactor{}))
	require.Equal(t, "string 1", string1)
	require.Equal(t, "string 1", foo.A)
	require.Equal(t, "_", string2)
	require.Equal(t, "_", *foo.B)
	require.Equal(t, "string 3", foo.C["3"])
}
