// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package os

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recorder"
	"github.com/cockroachdb/errors/oserror"
)

// OS is a convenience wrapper around the stdlib os package. It lets us:
//
// (a) mock operating system calls in tests, and
// (b) capture the set of calls that take place during execution
//
// We achieve (a) by embedding a Recorder, and either replaying from it if
// configured to do so, or "doing the real thing" and recording the fact into
// the Recorder for future playback.
//
// For (b), each operation is logged (if configured to do so). These messages
// can be captured by the caller and compared against what is expected.
type OS struct {
	dir    string
	logger *log.Logger
	*recorder.Recorder
}

// New constructs a new OS handle, configured with the provided options.
func New(opts ...Option) *OS {
	e := &OS{}

	// Apply the default options.
	defaults := []func(executor *OS){
		WithLogger(log.New(os.Stdout, "executing: ", 0)),
	}
	for _, opt := range defaults {
		opt(e)
	}

	// Apply the user-provided options, overriding the defaults as necessary.
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Option is a start-up option that can be specified when constructing a new OS
// handle.
type Option func(o *OS)

// WithLogger configures OS to use the provided logger.
func WithLogger(logger *log.Logger) func(o *OS) {
	return func(o *OS) {
		o.logger = logger
	}
}

// WithRecorder configures OS to use the provided recorder.
func WithRecorder(r *recorder.Recorder) func(o *OS) {
	return func(o *OS) {
		o.Recorder = r
	}
}

// WithWorkingDir configures Exec to use the provided working directory.
func WithWorkingDir(dir string) func(o *OS) {
	return func(o *OS) {
		o.dir = dir
	}
}

// MkdirAll wraps around os.MkdirAll, creating a directory named path, along
// with any necessary parents.
func (o *OS) MkdirAll(path string) error {
	command := fmt.Sprintf("mkdir %s", path)
	o.logger.Print(command)

	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		if err := os.MkdirAll(path, 0755); err != nil {
			return err
		}
	}

	if o.Recorder == nil {
		return nil
	}

	if o.Recording() {
		return o.record(command, "")
	}
	_, err := o.replay(command)
	return err
}

// Remove wraps around os.Remove, removing the named file or (empty) directory.
func (o *OS) Remove(path string) error {
	command := fmt.Sprintf("rm %s", path)
	o.logger.Print(command)

	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		if err := os.Remove(path); err != nil && !oserror.IsNotExist(err) {
			return err
		}
	}

	if o.Recorder == nil {
		return nil
	}

	if o.Recording() {
		return o.record(command, "")
	}
	_, err := o.replay(command)
	return err
}

// Symlink wraps around os.Symlink, creating a symbolic link to and from the
// named paths.
func (o *OS) Symlink(to, from string) error {
	command := fmt.Sprintf("ln -s %s %s", to, from)
	o.logger.Print(command)

	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		if err := os.Symlink(to, from); err != nil {
			return err
		}
	}

	if o.Recorder == nil {
		return nil
	}

	if o.Recording() {
		return o.record(command, "")
	}
	_, err := o.replay(command)
	return err
}

// Getwd wraps around os.Getwd, returning a rooted path name corresponding to
// the current directory.
func (o *OS) Getwd() (dir string, err error) {
	command := "getwd"
	o.logger.Print(command)

	if o.Recorder == nil || o.Recorder.Recording() {
		if o.dir != "" {
			dir = o.dir
		} else {
			// Do the real thing.
			dir, err = os.Getwd()
			if err != nil {
				return "", err
			}
		}
	}

	if o.Recorder == nil {
		return dir, nil
	}

	// We have to massage the output here to fit into a form that the recorder
	// understands. The recorder expects outputs to be terminated, so we'll do
	// that when recording, and we'll strip it back out when replaying from it.
	if o.Recording() {
		err := o.record(command, fmt.Sprintf("%s\n", dir))
		if err != nil {
			return "", err
		}
		return dir, nil
	}
	dir, err = o.replay(command)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dir), nil
}

// WriteFile wraps around os.Getwd, returning a rooted path name corresponding to
// the current directory.
func (o *OS) WriteFile(filename string, data []byte) error {
	command := fmt.Sprintf("writefile: %s", filename)
	o.logger.Print(command)

	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		if err := ioutil.WriteFile(filename, data, 0644); err != nil {
			return err
		}
	}

	if o.Recorder == nil {
		return nil
	}

	if o.Recording() {
		return o.record(command, "")
	}
	_, err := o.replay(command)
	return err
}

// replay replays the specified command, erroring out if it's mismatched with
// what the recorder plays back next. It returns the recorded output.
func (o *OS) replay(command string) (output string, err error) {
	found, err := o.Recorder.Next(func(op recorder.Operation) error {
		if op.Command != command {
			return fmt.Errorf("expected %q, got %q", op.Command, command)
		}
		output = op.Output
		return nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("recording for %q not found", command)
	}
	return output, nil
}

// record records the specified command.
func (o *OS) record(command, output string) error {
	op := recorder.Operation{
		Command: command,
		Output:  output,
	}

	return o.Record(op)
}
