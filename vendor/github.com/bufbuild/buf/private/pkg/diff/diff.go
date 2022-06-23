// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package diff implements diffing.
//
// Should primarily be used for testing.
package diff

// Largely copied from https://github.com/golang/go/blob/master/src/cmd/gofmt/gofmt.go
//
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// https://github.com/golang/go/blob/master/LICENSE

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// Diff does a diff.
//
// Returns nil if no diff.
func Diff(
	ctx context.Context,
	b1 []byte,
	b2 []byte,
	filename1 string,
	filename2 string,
	options ...DiffOption,
) ([]byte, error) {
	diffOptions := newDiffOptions()
	for _, option := range options {
		option(diffOptions)
	}
	return doDiff(
		ctx,
		b1,
		b2,
		filename1,
		filename2,
		diffOptions.suppressCommands,
		diffOptions.suppressTimestamps,
	)
}

// DiffOption is an option for Diff.
type DiffOption func(*diffOptions)

// DiffWithSuppressCommands returns a new DiffOption that suppresses printing of commands.
func DiffWithSuppressCommands() DiffOption {
	return func(diffOptions *diffOptions) {
		diffOptions.suppressCommands = true
	}
}

// DiffWithSuppressCommands returns a new DiffOption that suppresses printing of timestamps.
func DiffWithSuppressTimestamps() DiffOption {
	return func(diffOptions *diffOptions) {
		diffOptions.suppressTimestamps = true
	}
}

func doDiff(
	ctx context.Context,
	b1 []byte,
	b2 []byte,
	filename1 string,
	filename2 string,
	suppressCommands bool,
	suppressTimestamps bool,
) ([]byte, error) {
	if bytes.Equal(b1, b2) {
		return nil, nil
	}

	f1, err := writeTempFile("", "", b1)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.Remove(f1)
	}()

	f2, err := writeTempFile("", "", b2)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.Remove(f2)
	}()

	binaryPath := "diff"
	if runtime.GOOS == "plan9" {
		binaryPath = "/bin/ape/diff"
	}

	buffer := bytes.NewBuffer(nil)
	cmd := exec.CommandContext(ctx, binaryPath, "-u", f1, f2)
	cmd.Stdout = buffer
	cmd.Stderr = buffer
	err = cmd.Run()
	data := buffer.Bytes()
	if len(data) > 0 {
		// diff exits with a non-zero status when the files don't match.
		// Ignore that failure as long as we get output.
		return tryModifyHeader(data, filename1, filename2, suppressCommands, suppressTimestamps), nil
	}
	return nil, err
}

func writeTempFile(dir string, prefix string, data []byte) (string, error) {
	file, err := os.CreateTemp(dir, prefix)
	if err != nil {
		return "", err
	}
	if len(data) > 0 {
		_, err = file.Write(data)
	}
	if err1 := file.Close(); err == nil {
		err = err1
	}
	if err != nil {
		_ = os.Remove(file.Name())
		return "", err
	}
	return file.Name(), nil
}

func tryModifyHeader(
	diff []byte,
	filename1 string,
	filename2 string,
	suppressCommands bool,
	suppressTimestamps bool,
) []byte {
	bs := bytes.SplitN(diff, []byte{'\n'}, 3)
	if len(bs) < 3 {
		return diff
	}
	// Preserve timestamps.
	var t0, t1 []byte
	if !suppressTimestamps {
		if i := bytes.LastIndexByte(bs[0], '\t'); i != -1 {
			t0 = bs[0][i:]
		}
		if i := bytes.LastIndexByte(bs[1], '\t'); i != -1 {
			t1 = bs[1][i:]
		}
	}
	// Always print filepath with slash separator.
	filename1 = filepath.ToSlash(filename1)
	filename2 = filepath.ToSlash(filename2)
	if filename1 == filename2 {
		filename1 = filename1 + ".orig"
	}
	bs[0] = []byte(fmt.Sprintf("--- %s%s", filename1, t0))
	bs[1] = []byte(fmt.Sprintf("+++ %s%s", filename2, t1))
	if !suppressCommands {
		bs = append(
			[][]byte{
				[]byte(fmt.Sprintf("diff -u %s %s", filename1, filename2)),
			},
			bs...,
		)
	}
	return bytes.Join(bs, []byte{'\n'})
}

type diffOptions struct {
	suppressCommands   bool
	suppressTimestamps bool
}

func newDiffOptions() *diffOptions {
	return &diffOptions{}
}
