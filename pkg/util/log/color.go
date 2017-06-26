// Copyright 2013 Google Inc. All Rights Reserved.
// Copyright 2017 The Cockroach Authors.
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

package log

import (
	"os"
	"runtime"
	"strings"
)

// colorProfile defines escape sequences which provide color in
// terminals. Some terminals support 8 colors, some 256, others
// none at all.
type colorProfile struct {
	infoPrefix  []byte
	warnPrefix  []byte
	errorPrefix []byte
	timePrefix  []byte
}

var colorReset = []byte("\033[0m")

// For terms with 8-color support.
var colorProfile8 = &colorProfile{
	infoPrefix:  []byte("\033[0;36;49m"),
	warnPrefix:  []byte("\033[0;33;49m"),
	errorPrefix: []byte("\033[0;31;49m"),
	timePrefix:  []byte("\033[2;37;49m"),
}

// For terms with 256-color support.
var colorProfile256 = &colorProfile{
	infoPrefix:  []byte("\033[38;5;33m"),
	warnPrefix:  []byte("\033[38;5;214m"),
	errorPrefix: []byte("\033[38;5;160m"),
	timePrefix:  []byte("\033[38;5;246m"),
}

// the --no-color flag.
var noColor bool

var stderrColorProfile = func() *colorProfile {
	if noColor {
		return nil
	}

	// Determine whether stderr is a character device and if so, that
	// the terminal supports color output.

	fi, err := OrigStderr.Stat()
	if err != nil {
		// Stat() will return an error on Windows in both Powershell and
		// console until go1.9. See https://github.com/golang/go/issues/14853.
		//
		// Note that this bug does not affect MSYS/Cygwin terminals.
		//
		// TODO(bram): remove this hack once we move to go 1.9.
		//
		// Console does not support our color profiles but
		// Powershell supports colorProfile256. Sadly, detecting the
		// shell is not well supported, so default to no-color.
		if runtime.GOOS != "windows" {
			panic(err)
		}
		return nil
	}
	if (fi.Mode() & os.ModeCharDevice) != 0 {
		term := os.Getenv("TERM")
		switch term {
		case "ansi", "tmux":
			return colorProfile8
		case "st":
			return colorProfile256
		default:
			if strings.HasSuffix(term, "256color") {
				return colorProfile256
			}
			if strings.HasSuffix(term, "color") || strings.HasPrefix(term, "screen") {
				return colorProfile8
			}
		}
	}
	return nil
}()
