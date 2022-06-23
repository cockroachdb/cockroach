// Copyright 2021 Irfan Sharif.
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

//go:build gofuzz
// +build gofuzz

package recorder

import (
	"bytes"
	"fmt"
)

func Fuzz(data []byte) int {
	reader := New(WithReplay(bytes.NewReader(data), "fuzz"))
	parsedOnce := false
	for {
		// Parse out the next operation.
		var output, command string
		found, err := reader.step(func(op operation) {
			command, output = op.command, op.output
		})
		if err != nil {
			if output != "" || command != "" || found {
				panic("found non-empty command, despite error")
			}
			return 0
		}
		if !found {
			// We're at the end of the file.
			if parsedOnce {
				return 1
			} else {
				return 0
			}
		} else {
			parsedOnce = true
		}

		// Write out the next operation, just to see that it goes through.
		buffer := bytes.NewBuffer(nil)
		writer := New(WithRecording(buffer))
		if err := writer.record(operation{command, output}); err != nil {
			panic(err)
		}

		// Re-read what we just wrote out, just to see we're able to round trip
		// through the recorder.
		reader2 := New(WithReplay(buffer, "fuzz"))
		_, err = reader2.step(func(op operation) {
			if op.command != command {
				panic(fmt.Sprintf("mismatched command: expected %q, got %q", command, op.command))
			}
			if op.output != output {
				panic(fmt.Sprintf("mismatched output: expected %q, got %q", output, op.output))
			}
		})
		if err != nil {
			panic(err)
		}
	}
}
