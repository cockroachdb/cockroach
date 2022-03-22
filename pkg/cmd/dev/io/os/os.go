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
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/irfansharif/recorder"
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

	knobs struct { // testing knobs
		dryrun bool
	}
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

// WithWorkingDir configures OS to use the provided working directory.
func WithWorkingDir(dir string) func(o *OS) {
	return func(o *OS) {
		o.dir = dir
	}
}

// WithDryrun configures OS to run in dryrun mode.
func WithDryrun() func(e *OS) {
	return func(e *OS) {
		e.knobs.dryrun = true
	}
}

// MkdirAll wraps around os.MkdirAll, creating a directory named path, along
// with any necessary parents.
func (o *OS) MkdirAll(path string) error {
	command := fmt.Sprintf("mkdir %s", path)
	o.logger.Print(command)

	_, err := o.Next(command, func() (output string, err error) {
		return "", os.MkdirAll(path, 0755)
	})
	return err
}

// Remove wraps around os.Remove, removing the named file or (empty) directory.
func (o *OS) Remove(path string) error {
	command := fmt.Sprintf("rm %s", path)
	o.logger.Print(command)

	_, err := o.Next(command, func() (output string, err error) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return "", err
		}
		return "", nil
	})
	return err
}

// Symlink wraps around os.Symlink, creating a symbolic link to and from the
// named paths.
func (o *OS) Symlink(to, from string) error {
	command := fmt.Sprintf("ln -s %s %s", to, from)
	o.logger.Print(command)

	_, err := o.Next(command, func() (output string, err error) {
		return "", os.Symlink(to, from)
	})
	return err
}

// Getenv wraps around os.Getenv, retrieving the value of the environment
// variable named by the key.
func (o OS) Getenv(key string) string {
	command := fmt.Sprintf("getenv %s", key)
	o.logger.Print(command)

	output, err := o.Next(command, func() (output string, err error) {
		return os.Getenv(key), nil
	})
	if err != nil {
		log.Fatalf("%v", err)
	}
	return output
}

// Setenv wraps around os.Setenv, which sets the value of the environment
// variable named by the key. It returns an error, if any.
func (o *OS) Setenv(key, value string) error {
	command := fmt.Sprintf("export %s=%s", key, value)
	o.logger.Print(command)

	_, err := o.Next(command, func() (output string, err error) {
		return "", os.Setenv(key, value)
	})
	return err
}

// Readlink wraps around os.Readlink, which returns the destination of the named
// symbolic link. If there is an error, it will be of type *PathError.
func (o *OS) Readlink(filename string) (string, error) {
	command := fmt.Sprintf("readlink %s", filename)
	o.logger.Print(command)

	return o.Next(command, func() (output string, err error) {
		return os.Readlink(filename)
	})
}

// IsDir wraps around os.Stat, which returns the os.FileInfo of the named
// directory. IsDir returns true if and only if it is an existing directory.
// If there is an error, it will be of type *PathError.
func (o *OS) IsDir(dirname string) (bool, error) {
	command := fmt.Sprintf("find %s -type d", dirname)
	o.logger.Print(command)

	output, err := o.Next(command, func() (output string, err error) {
		// Do the real thing.
		stat, err := os.Stat(dirname)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(stat.IsDir()), nil
	})
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(strings.TrimSpace(output))
}

// ReadFile wraps around ioutil.ReadFile, reading a file from disk and
// returning the contents.
func (o *OS) ReadFile(filename string) (string, error) {
	command := fmt.Sprintf("cat %s", filename)
	o.logger.Print(command)

	return o.Next(command, func() (output string, err error) {
		buf, err := ioutil.ReadFile(filename)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	})
}

// WriteFile wraps around ioutil.ReadFile, writing the given contents to
// the given file on disk.
func (o *OS) WriteFile(filename, contents string) error {
	var command string
	{
		commandContents := contents
		if len(commandContents) > 10 {
			commandContents = commandContents[:10] // keeps the logging manageable
		}
		command = fmt.Sprintf("echo %q > %s", strings.TrimSpace(commandContents), filename)
	}
	o.logger.Print(command)

	_, err := o.Next(command, func() (output string, err error) {
		return "", ioutil.WriteFile(filename, []byte(contents), 0666)
	})
	return err
}

// CopyFile copies a file from one location to another.
// In practice we frequently use this function to copy `src` to `dst`
// where `src` is a symlink to the already-existing file `dst`; a naive
// implementation would wipe `dst` (and `src` accordingly).
// Unlike a simple io.Copy, this function checks for that case and is a
// no-op if `src` is already a symlink to `dst`.
func (o *OS) CopyFile(src, dst string) error {
	command := fmt.Sprintf("cp %s %s", src, dst)
	o.logger.Print(command)

	_, err := o.Next(command, func() (output string, err error) {
		srcFile, err := os.Open(src)
		if err != nil {
			return "", err
		}
		defer func() { _ = srcFile.Close() }()
		originalDstFile, err := os.Open(dst)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		} else if err == nil {
			defer func() { _ = originalDstFile.Close() }()
			srcInfo, err := srcFile.Stat()
			if err != nil {
				return "", err
			}
			dstInfo, err := originalDstFile.Stat()
			if err != nil {
				return "", err
			}
			// If src points to the same file as dst, there's
			// nothing to be done.
			if os.SameFile(srcInfo, dstInfo) {
				return "", nil
			}
		}
		dstFile, err := os.Create(dst)
		if err != nil {
			return "", err
		}
		defer func() { _ = dstFile.Close() }()
		_, err = io.Copy(dstFile, srcFile)
		return "", err
	})
	return err
}

// ListFilesWithSuffix lists all the files under a directory recursively that
// end in the given suffix.
func (o *OS) ListFilesWithSuffix(root, suffix string) ([]string, error) {
	command := fmt.Sprintf("find %s -name *%s", root, suffix)
	o.logger.Print(command)

	output, err := o.Next(command, func() (output string, err error) {
		var ret []string
		if err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
			// If there's an error walking the tree, throw it away -- there's
			// nothing interesting we can do with it.
			if err != nil || info.IsDir() {
				//nolint:returnerrcheck
				return nil
			}
			if strings.HasSuffix(path, suffix) {
				ret = append(ret, path)
			}
			return nil
		}); err != nil {
			return "", err
		}

		return fmt.Sprintf("%s\n", strings.Join(ret, "\n")), nil
	})
	if err != nil {
		return nil, err
	}
	return strings.Split(strings.TrimSpace(output), "\n"), nil
}

// CurrentUserAndGroup returns the user and effective group.
func (o *OS) CurrentUserAndGroup() (uid string, gid string, err error) {
	command := "id"
	o.logger.Print(command)

	output, err := o.Next(command, func() (output string, err error) {
		current, err := user.Current()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s:%s", current.Uid, current.Gid), nil
	})
	if err != nil {
		return "", "", err
	}

	ids := strings.Split(strings.TrimSpace(output), ":")
	return ids[0], ids[1], nil
}

// Next is a thin interceptor for all os activity, running them through
// testing knobs first.
func (o *OS) Next(command string, f func() (output string, err error)) (string, error) {
	if o.knobs.dryrun {
		return "", nil
	}
	return o.Recorder.Next(command, f)
}
