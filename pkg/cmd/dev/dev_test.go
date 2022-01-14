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
	"bytes"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/os"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recording"
	"github.com/stretchr/testify/require"
)

func init() {
	isTesting = true
}

func TestSetupPath(t *testing.T) {
	rec := `getenv PATH
----
/usr/local/opt/ccache/libexec:/usr/local/opt/make/libexec/gnubin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/go/bin:/Library/Apple/usr/bin

which cc
----
/usr/local/opt/ccache/libexec/cc

readlink /usr/local/opt/ccache/libexec/cc
----
../bin/ccache

export PATH=/usr/local/opt/make/libexec/gnubin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/go/bin:/Library/Apple/usr/bin
----

`
	r := recording.WithReplayFrom(strings.NewReader(rec), "TestSetupPath")
	var logger io.ReadWriter = bytes.NewBufferString("")
	var exopts []exec.Option
	exopts = append(exopts, exec.WithRecording(r))
	exopts = append(exopts, exec.WithLogger(log.New(logger, "", 0)))
	var osopts []os.Option
	osopts = append(osopts, os.WithRecording(r))
	osopts = append(osopts, os.WithLogger(log.New(logger, "", 0)))
	devExec := exec.New(exopts...)
	os := os.New(osopts...)
	dev := makeDevCmd()
	dev.exec = devExec
	dev.os = os

	require.NoError(t, setupPath(dev))
}
