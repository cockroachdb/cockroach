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

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recording"
	"github.com/cockroachdb/errors/oserror"
)

// OS is a convenience wrapper around the stdlib os package. It lets us
// mock operating system calls in tests.
type OS struct {
	logger *log.Logger
	*recording.Recording
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

// WithRecording configures OS to use the provided recording.
func WithRecording(r *recording.Recording) func(o *OS) {
	return func(o *OS) {
		o.Recording = r
	}
}

// MkdirAll wraps around os.MkdirAll, creating a directory named path, along
// with any necessary parents.
func (o *OS) MkdirAll(path string) error {
	command := fmt.Sprintf("mkdir %s", path)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		if err := os.MkdirAll(path, 0755); err != nil {
			return err
		}
		return nil
	}

	_, err := o.replay(command)
	return err
}

// Remove wraps around os.Remove, removing the named file or (empty) directory.
func (o *OS) Remove(path string) error {
	command := fmt.Sprintf("rm %s", path)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		if err := os.Remove(path); err != nil && !oserror.IsNotExist(err) {
			return err
		}
		return nil
	}

	_, err := o.replay(command)
	return err
}

// Symlink wraps around os.Symlink, creating a symbolic link to and from the
// named paths.
func (o *OS) Symlink(to, from string) error {
	command := fmt.Sprintf("ln -s %s %s", to, from)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		if err := os.Symlink(to, from); err != nil {
			return err
		}
		return nil
	}

	_, err := o.replay(command)
	return err
}

// Getenv wraps around os.Getenv, retrieving the value of the environment
// variable named by the key.
func (o OS) Getenv(key string) string {
	command := fmt.Sprintf("getenv %s", key)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		return os.Getenv(key)
	}

	ret, _ := o.replay(command)
	return ret
}

// Setenv wraps around os.Setenv, which sets the value of the environment
// variable named by the key. It returns an error, if any.
func (o *OS) Setenv(key, value string) error {
	command := fmt.Sprintf("export %s=%s", key, value)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		return os.Setenv(key, value)
	}

	_, err := o.replay(command)
	return err
}

// Readlink wraps around os.Readlink, which returns the destination of the named
// symbolic link. If there is an error, it will be of type *PathError.
func (o *OS) Readlink(filename string) (string, error) {
	command := fmt.Sprintf("readlink %s", filename)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		return os.Readlink(filename)
	}

	ret, err := o.replay(command)
	return ret, err
}

// ReadFile wraps around ioutil.ReadFile, reading a file from disk and
// returning the contents.
func (o *OS) ReadFile(filename string) (string, error) {
	command := fmt.Sprintf("cat %s", filename)
	o.logger.Print(command)

	if o.Recording == nil {
		// Do the real thing.
		buf, err := ioutil.ReadFile(filename)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	}

	ret, err := o.replay(command)
	return ret, err
}

// replay replays the specified command, erroring out if it's mismatched with
// what the recording plays back next. It returns the recorded output.
func (o *OS) replay(command string) (output string, err error) {
	found, err := o.Recording.Next(func(op recording.Operation) error {
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
