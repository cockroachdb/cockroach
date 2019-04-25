// Copyright 2019 The Cockroach Authors.
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

package errbase

import "fmt"

func (e *opaqueWrapper) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", e.cause)
			fmt.Fprintf(s, "\n-- prefix: %s", e.prefix)
			fmt.Fprintf(s, "\n-- type name: %s", e.typeName)
			for i, d := range e.safeDetails {
				fmt.Fprintf(s, "\n-- reportable %d:\n%s", i, d)
			}
			if e.payload != nil {
				fmt.Fprintf(s, "\n-- payload type: %s", e.payload.TypeUrl)
			}
			return
		}
		fallthrough
	case 's', 'q':
		FormatError(s, verb, e.cause)
	}
}

func (e *opaqueLeaf) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%s", e.msg)
			fmt.Fprintf(s, "\n-- type name: %s", e.typeName)
			for i, d := range e.safeDetails {
				fmt.Fprintf(s, "\n-- reportable %d:\n%s", i, d)
			}
			if e.payload != nil {
				fmt.Fprintf(s, "\n-- payload type: %s", e.payload.TypeUrl)
			}
			return
		}
		fallthrough
	case 's', 'q':
		fmt.Fprintf(s, fmt.Sprintf("%%%c", verb), e.msg)
	}
}

// FormatError is a helper function that formats the given error using
// the given verb and the + flag if currently set in the formatting
// state.
func FormatError(s fmt.State, verb rune, err error) {
	flags := ""
	if s.Flag('+') {
		flags += "+"
	}
	fmtString := fmt.Sprintf("%%%s%c", flags, verb)
	fmt.Fprintf(s, fmtString, err)
}
