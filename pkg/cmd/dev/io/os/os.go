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

// Getenv wraps around os.Getenv, retrieving the value of the environment
// variable named by the key.
func (o OS) Getenv(key string) string {
	command := fmt.Sprintf("getenv %s", key)
	o.logger.Print(command)

	var env string
	if o.Recorder == nil || o.Recorder.Recording() {
		env = os.Getenv(key)
	}

	if o.Recorder == nil {
		return env
	}

	if o.Recording() {
		err := o.record(command, env)
		if err != nil {
			return ""
		}
		return env
	}
	ret, _ := o.replay(command)
	return ret
}

// Setenv wraps around os.Setenv, which sets the value of the environment
// variable named by the key. It returns an error, if any.
func (o *OS) Setenv(key, value string) error {
	command := fmt.Sprintf("export %s=%s", key, value)
	o.logger.Print(command)

	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		if err := os.Setenv(key, value); err != nil {
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

// Readlink wraps around os.Readlink, which returns the destination of the named
// symbolic link. If there is an error, it will be of type *PathError.
func (o *OS) Readlink(filename string) (string, error) {
	command := fmt.Sprintf("readlink %s", filename)
	o.logger.Print(command)

	var resolved string
	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		var err error
		resolved, err = os.Readlink(filename)
		if err != nil {
			return "", err
		}
	}

	if o.Recorder == nil {
		return resolved, nil
	}

	if o.Recording() {
		return resolved, o.record(command, resolved)
	}
	ret, err := o.replay(command)
	return ret, err
}

// ReadFile wraps around ioutil.ReadFile, reading a file from disk and
// returning the contents.
func (o *OS) ReadFile(filename string) (string, error) {
	command := fmt.Sprintf("cat %s", filename)
	o.logger.Print(command)

	var out string
	if o.Recorder == nil || o.Recorder.Recording() {
		// Do the real thing.
		buf, err := ioutil.ReadFile(filename)
		if err != nil {
			return "", err
		}
		out = string(buf)
	}

	if o.Recorder == nil {
		return out, nil
	}

	if o.Recording() {
		return out, o.record(command, out)
	}
	ret, err := o.replay(command)
	return ret, err
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
