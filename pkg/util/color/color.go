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
// The API exposes two programming styles. In the first style, functions print
// directly to the output stream:
//
//     color.Stdout(color.Red)
//     fmt.Println("Red text")
//     color.Stdout(color.Reset)
//
// In the second style, functions return a byte slice containing the escape
// sequence:
//
//     fmt.Printf("%sRed text%s\n", color.StdoutProfile.Red(), color.StdoutProfile.Reset())
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
package color

import (
	"os"
	"runtime"
	"strings"
)

// Code represents a terminal color code.
type Code int

// Color codes.
const (
	Red Code = iota
	Yellow
	Cyan
	Light
	Reset
)

// Profile defines escape sequences which provide color in terminals. Some
// terminals support 8 colors, some 256, others none at all.
type Profile [][]byte

// For terminals with 8-color support.
var profile8 = Profile{
	// Keep these in the same order as the color codes above.
	[]byte("\033[0;31;49m"),
	[]byte("\033[0;33;49m"),
	[]byte("\033[0;36;49m"),
	[]byte("\033[2;37;49m"),
	[]byte("\033[0m"),
}

// For terminals with 256-color support.
var profile256 = Profile{
	// Keep these in the same order as the color codes above.
	[]byte("\033[38;5;160m"),
	[]byte("\033[38;5;214m"),
	[]byte("\033[38;5;33m"),
	[]byte("\033[38;5;246m"),
	[]byte("\033[0m"),
}

func (p Profile) get(code Code) []byte {
	if p == nil {
		return nil
	}
	return p[code]
}

// Red returns the escape sequence to produce red text.
func (p Profile) Red() []byte { return p.get(Red) }

// Yellow returns the escape sequence to produce yellow text.
func (p Profile) Yellow() []byte { return p.get(Yellow) }

// Cyan returns the escape sequence to produce cyan text.
func (p Profile) Cyan() []byte { return p.get(Cyan) }

// Light returns the escape sequence to produce light text.
func (p Profile) Light() []byte { return p.get(Light) }

// Reset returns the escape sequence to produce reset text.
func (p Profile) Reset() []byte { return p.get(Reset) }

// Stdout sets the color for future output to os.Stdout.
func Stdout(code Code) {
	if StdoutProfile == nil {
		return
	}
	os.Stdout.Write(StdoutProfile[code])
}

// Stderr sets the color for future output to os.Stderr.
func Stderr(code Code) {
	if StderrProfile == nil {
		return
	}
	os.Stderr.Write(StderrProfile[code])
}

func detectProfile(f *os.File) Profile {
	// Console does not support our color profiles but Powershell supports
	// profile256. Sadly, detecting the shell is not well supported, so default to
	// no-color.
	if runtime.GOOS == "windows" {
		return nil
	}

	// Determine whether f is a character device and if so, that the terminal
	// supports color output.
	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}
	if (fi.Mode() & os.ModeCharDevice) != 0 {
		term := os.Getenv("TERM")
		switch term {
		case "ansi", "tmux":
			return profile8
		case "st":
			return profile256
		default:
			if strings.HasSuffix(term, "256color") {
				return profile256
			}
			if strings.HasSuffix(term, "color") || strings.HasPrefix(term, "screen") {
				return profile8
			}
		}
	}
	return nil
}

// StdoutProfile is the Profile to use for stdout.
var StdoutProfile = detectProfile(os.Stdout)

// StderrProfile is the Profile to use for stderr.
var StderrProfile = detectProfile(os.Stderr)
