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

package template

import (
	"bufio"
	"io"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var commandRegexp = regexp.MustCompile(`\A\s*//\s*@`)

// Parse parses a template.
func Parse(r io.Reader) (*Template, error) {
	var stack []*Template
	top := &Template{typ: blockStatement}
	s := bufio.NewScanner(r)
	for lineNumber := 1; s.Scan(); lineNumber++ {
		line := s.Text()
		loc := commandRegexp.FindStringIndex(line)
		if loc == nil {
			top.children = append(top.children, &Template{typ: echoStatement, arg: line})
			continue
		}
		fields := strings.Fields(line[loc[1]:])
		if len(fields) != 2 {
			return nil, errorLocation("invalid directive", lineNumber)
		}
		directive, label := fields[0], fields[1]
		var begin bool
		var typ statementType
		switch directive {
		case "done":
			begin = false
			typ = forStatement
		case "fi":
			begin = false
			typ = ifStatement
		case "for":
			begin = true
			typ = forStatement
		case "if":
			begin = true
			typ = ifStatement
		default:
			return nil, errorLocation("unrecognized directive", lineNumber)
		}
		if begin {
			stack = append(stack, top)
			top = &Template{typ: typ, arg: label}
			continue
		}
		if len(stack) == 0 {
			return nil, errorLocation("unexpected end directive", lineNumber)
		}
		if typ != top.typ {
			return nil, errorLocation("end directive does not match beginning", lineNumber)
		}
		if label != top.arg {
			return nil, errorLocation("end label does not match beginning", lineNumber)
		}
		var parent *Template
		n := len(stack) - 1
		stack, parent = stack[:n], stack[n]
		parent.children = append(parent.children, top)
		top = parent
	}
	if s.Err() != nil {
		return nil, s.Err()
	}
	if len(stack) > 0 {
		return nil, errors.New("unexpected end of input")
	}
	return top, nil
}

func errorLocation(message string, lineNumber int) error {
	return errors.Wrapf(errors.New(message), "line %d", lineNumber)
}
