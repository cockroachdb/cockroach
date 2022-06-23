// Copyright 2021 Irfan Sharif.
// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Portions of this code was derived from cockroachdb/datadriven.

package recorder

import (
	"errors"
	"fmt"
	"io"
	"log"
)

// Recorder can be used to record a set of operations (defined only by a
// "command" and an "output"; see grammar below). These recordings can
// then be played back, which provides a handy way to mock out the components
// being recorded.
//
// Users will typically want to embed a Recorder into structs that oversee the
// sort of side-effect or I/O they'd like to record and later playback (instead
// of "doing the real thing" in tests). These side-effects could be pretty much
// anything. If we're building a CLI that calls into the filesystem to filter
// for a set of files and writes out their contents to a zip file, the "I/O"
// would be the listing out of files, and the side-effects would include
// writing the zip file.
//
// I/O could also be outside-of-package boundaries that a stand-alone component
// calls out to. Recorders, if embedded into the component in question, lets us:
//  (a) Record the set of outbound calls, and the relevant responses, while
//  "doing the real thing".
//  (b) Play back from earlier recordings, intercepting all outbound calls and
//  effecting mock out all dependencies the component has.
//
// Let us try and mock out a globber. Broadly what it could look like is as
// follows:
//
//      type globber struct {
//          *recorder.Recorder
//      }
//
//      // glob returns the names of all files matching the given pattern.
//      func (g *globber) glob(pattern string) ([]string, error) {
//          output, err := g.Next(pattern, func() (string, error) {
//              matches, err := filepath.Glob(pattern) // do the real thing
//              if err != nil {
//                  return "", err
//              }
//
//              output := fmt.Sprintf("%s\n", strings.Join(matches, "\n"))
//              return output, nil
//          })
//          if err != nil {
//              return nil, err
//          }
//
//          matches := strings.Split(strings.TrimSpace(output), "\n")
//          return matches, nil
//      }
//
// We had to define tiny bi-directional parsers to convert our input and output
// to the human-readable string form Recorders understand. Strung together we
// can build tests that would plumb in Recorders with the right mode and play
// back from them if asked for. See example/ for this test pattern, where it
// behaves differently depending on whether or not -record is specified.
//
// 		$ go test -run TestExample [-record]
//		$ cat testdata/recording
// 		testdata/files/*
// 		----
// 		testdata/files/aaa
// 		testdata/files/aab
// 		testdata/files/aac
//
// Once the recordings are captured, they can be edited and maintained by hand.
// An example of where we might want to do this is for recordings for commands
// that generate copious amounts of output. It suffices for us to trim the
// recording down by hand, and make sure we don't re-record over it (by
// inspecting the diffs during review). Recordings, like other mocks, are also
// expected to get checked in as test data fixtures.
//
// ---
//
// The printed form of the command is defined by the following grammar. This
// form is used when generating/reading from recording files.
//
//   # comment
//   <command> \
//   <that wraps over onto the next line>
//   ----
//   <output>
//
// By default <output> cannot contain blank lines. This alternative syntax
// allows the use of blank lines.
//
//   <command>
//   ----
//   ----
//   <output>
//
//   <more output>
//   ----
//   ----
//
// Callers are free to use <output> to model errors as well; it's all opaque to
// Recorders.
type Recorder struct {
	// writer is set if we're in recording mode, and is where operations are
	// recorded.
	writer io.Writer

	// scanner and op are set if we're in replay mode. It's where we're
	// replaying the recording from. op is the scratch space used to
	// parse out the current operation being read.
	scanner *scanner
	op      operation
}

// New constructs a Recorder, using the specified configuration option (either
// WithReplay or WithRecording).
func New(opt Option) *Recorder {
	r := &Recorder{}
	opt(r)
	return r
}

// Option is used to configure a new Recorder.
type Option func(r *Recorder)

// WithReplay is used to configure a Recorder to play back from the given
// io.Reader. The provided name is used only for diagnostic purposes, it's
// typically the name of the recording file being read.
func WithReplay(from io.Reader, name string) Option {
	return func(re *Recorder) {
		re.scanner = newScanner(from, name)
	}
}

// WithRecording is used to configure a Recorder to record into the given
// io.Writer. The recordings can then later be replayed from (see WithReplay).
func WithRecording(to io.Writer) Option {
	return func(r *Recorder) {
		r.writer = to
	}
}

// Next is used to step through the next operation in the recorder. It does one
// of three things, depending on how the recorder is configured.
//  a. If the recorder is nil (i.e. it's simply not configured), it will
//     transparently execute the callback;
//  b. WithRecording records the given command and output (captured by the
//     provided callback);
//  c. WithReplay replays the next command in the recording, as long as it's
//     identical to the provided one.
//
// TODO(irfansharif): We could pass a boolean to the given callback to let
// callers distinguish between (a) and (b), helping avoid the overhead of
// pretty-printing + parsing when run outside the context of tests.
func (r *Recorder) Next(command string, f func() (output string, err error)) (string, error) {
	if r == nil {
		// (a) Do the real thing; we're not recording or replaying.
		output, err := f()
		return output, err
	}

	if r.recording() {
		// (b) We're recording, labeling with the given command name.
		output, err := f()
		if err != nil {
			return "", err
		}

		op := operation{command, output}
		if err := r.record(op); err != nil {
			log.Fatalf("%v", err)
		}
		return output, nil
	}

	// (c) We're replaying from the next command in the recording.
	var output string
	found, err := r.step(func(op operation) {
		if op.command != command {
			log.Fatalf("%s: expected: %q\ngot: %q\n\n"+
				"do you need to regenerate the recording using -record?",
				r.scanner.pos(), op.command, command)
		}
		output = op.output
	})
	if err != nil {
		log.Fatalf("%v", err)
	}
	if !found {
		log.Fatalf("%s: recording for %q not found\n\n"+
			"do you need to regenerate the recording using -record?",
			r.scanner.pos(), command)
	}

	return output, nil
}

// recording returns whether the recorder is configured to record (as opposed to
// being set to replay from an existing recording).
func (r *Recorder) recording() bool {
	return r.writer != nil
}

// record is used to record the given operation.
func (r *Recorder) record(op operation) error {
	if !r.recording() {
		return errors.New("misconfigured recorder: not set to record")
	}

	_, err := r.writer.Write([]byte(op.String()))
	if err != nil {
		return fmt.Errorf("unable to write recording for %q: %v", op.command, err)
	}
	return nil
}

// step is used to iterate through the next operation found in the recording, if
// any.
func (r *Recorder) step(f func(operation)) (found bool, err error) {
	if r.recording() {
		return false, errors.New("misconfigured recorder; set to record, not replay")
	}

	parsed, err := r.parseOperation()
	if err != nil {
		return false, fmt.Errorf("%s: unable to parse recording file: %v", r.scanner.name, err)
	}

	if !parsed {
		return false, nil
	}

	f(r.op)
	return true, nil
}
