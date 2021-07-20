// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseArgs(t *testing.T) {
	_, err := parseArgs([]string{}, -1)
	assert.True(t, errors.Is(err, errUsage))
	_, err = parseArgs([]string{"build"}, -1)
	assert.True(t, errors.Is(err, errUsage))
	_, err = parseArgs([]string{"typo", "target"}, -1)
	assert.NotNil(t, err)
	args, err := parseArgs([]string{"build", "target"}, -1)
	assert.Nil(t, err)
	assert.Equal(t, parsedArgs{
		subcmd:     "build",
		targets:    []string{"target"},
		additional: []string{}}, *args)
	args, err = parseArgs([]string{"test", "target1", "target2"}, -1)
	assert.Nil(t, err)
	assert.Equal(t, parsedArgs{
		subcmd:     "test",
		targets:    []string{"target1", "target2"},
		additional: []string{}}, *args)
	// Make sure additional arguments are captured correctly.
	args, err = parseArgs([]string{"test", "target1", "target2", "--verbose_failures"}, 3)
	assert.Nil(t, err)
	assert.Equal(t, parsedArgs{
		subcmd:     "test",
		targets:    []string{"target1", "target2"},
		additional: []string{"--verbose_failures"},
	}, *args)
}
