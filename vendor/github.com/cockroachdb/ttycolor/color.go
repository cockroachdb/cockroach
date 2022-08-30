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

// This code originated in the github.com/golang/glog package.

// Package color is a lightweight terminal color library. It is only compatible
// with terminals which support ANSI escape codes.
//
// The API exposes two programming styles. You can use functions that print the
// color escape sequences directly to the output stream:
//
//     color.Stdout(color.Red)
//     fmt.Println("Red text")
//     color.Stdout(color.Reset)
//
// Or, you can retrieve the color escape sequence by directly looking up a
// color.Code in a color.Profile:
//
//     fmt.Printf("%sRed text%s\n", color.StdoutProfile[color.Red],
//       color.StdoutProfile[color.Reset])
//
// Text destined for stderr should use color.Stderr and color.StderrProfile
// instead.
//
// When the output stream is not a character device, or the TERM environment
// variable indicates an unsupported terminal type, functions will print nothing
// or return a nil byte slice.
//
// While we link the main CockroachDB binary against libncurses or libterminfo,
// which bundle information about nearly every terminal ever produced, handling
// colors for non-ANSI terminals is no small task. See, for example, the
// difference between the setaf (set ANSI foreground) and setf (set foreground)
// capabilities. Since non-ANSI terminals are few and far between, it's not
// worth the trouble.
package ttycolor

import (
	"os"
	"runtime"
	"strings"
)

// Code represents a terminal color code.
type Code int

// Color codes.
const (
	Black Code = iota
	Red
	Green
	Yellow
	Blue
	Magenta
	Cyan
	White
	Gray
	Reset
)

// Profile defines escape sequences which provide color in terminals. Some
// terminals support 8 colors, some 256, others none at all.
type Profile map[Code][]byte

// Profile8 is suitable for terminals with 8-color support.
var Profile8 = Profile{
	// Keep these in the same order as the color codes above.
	Black:   []byte("\033[0;30;49m"),
	Red:     []byte("\033[0;31;49m"),
	Green:   []byte("\033[0;32;49m"),
	Yellow:  []byte("\033[0;33;49m"),
	Blue:    []byte("\033[0;34;49m"),
	Magenta: []byte("\033[0;35;49m"),
	Cyan:    []byte("\033[0;36;49m"),
	White:   []byte("\033[0;37;49m"),
	Gray:    []byte("\033[2;37;49m"),
	Reset:   []byte("\033[0m"),
}

// Profile256 is suitable for terminals with 256-color support.
var Profile256 = Profile{
	// Keep these in the same order as the color codes above.
	Black:   []byte("\033[38;5;0m"),
	Red:     []byte("\033[38;5;160m"),
	Green:   []byte("\033[38;5;2m"),
	Yellow:  []byte("\033[38;5;214m"),
	Blue:    []byte("\033[38;5;4m"),
	Magenta: []byte("\033[38;5;5m"),
	Cyan:    []byte("\033[38;5;33m"),
	White:   []byte("\033[38;5;315"),
	Gray:    []byte("\033[38;5;246m"),
	Reset:   []byte("\033[0m"),
}

// BackgroundColorSequence returns the corresponding color in the profile as
// a background color escape sequence.
func (cp Profile) BackgroundColorSequence(code Code) []byte {
	es := cp[code]
	const backgroundColorEscapeSequence = "\033[7m"
	return append(es[:len(es):len(es)], []byte(backgroundColorEscapeSequence)...)
}

// PickArbitraryColor maps a uint32 to an arbitrary color code (excluding Reset)
// in a deterministic fashion.
func PickArbitraryColor(input uint32) Code {
	return Code(input % uint32(Reset))
}

// Stdout sets the color for future output to os.Stdout.
func Stdout(code Code) {
	if StdoutProfile == nil {
		return
	}
	_, _ = os.Stdout.Write(StdoutProfile[code])
}

// Stderr sets the color for future output to os.Stderr.
func Stderr(code Code) {
	if StderrProfile == nil {
		return
	}
	_, _ = os.Stderr.Write(StderrProfile[code])
}

// DetectProfile configures a profile suitable for the given file output.
func DetectProfile(f *os.File) (Profile, error) {
	// Console does not support our color profiles but Powershell supports
	// Profile256. Sadly, detecting the shell is not well supported, so default to
	// no-color.
	if runtime.GOOS == "windows" {
		return nil, nil
	}

	// Determine whether f is a character device and if so, that the terminal
	// supports color output.
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if (fi.Mode() & os.ModeCharDevice) != 0 {
		term := os.Getenv("TERM")
		switch term {
		case "ansi", "tmux":
			return Profile8, nil
		case "st":
			return Profile256, nil
		default:
			if strings.HasSuffix(term, "256color") {
				return Profile256, nil
			}
			if strings.HasSuffix(term, "color") || strings.HasPrefix(term, "screen") {
				return Profile8, nil
			}
		}
	}
	return nil, nil
}

func detectProfileOrPanic(f *os.File) Profile {
	cp, err := DetectProfile(f)
	if err != nil {
		panic(err)
	}
	return cp
}

// StdoutProfile is the Profile to use for stdout.
var StdoutProfile = detectProfileOrPanic(os.Stdout)

// StderrProfile is the Profile to use for stderr.
var StderrProfile = detectProfileOrPanic(os.Stderr)
