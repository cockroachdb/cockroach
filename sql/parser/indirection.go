// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

// IndirectionElem is a single element in an indirection expression.
type IndirectionElem interface {
	indirectionElem()
	String() string
}

func (NameIndirection) indirectionElem()   {}
func (StarIndirection) indirectionElem()   {}
func (*ArrayIndirection) indirectionElem() {}

// Indirection represents an indirection expression composed of a series of
// indirection elements.
type Indirection []IndirectionElem

func (i Indirection) String() string {
	var buf bytes.Buffer
	for _, e := range i {
		buf.WriteString(e.String())
	}
	return buf.String()
}

// NameIndirection represents ".<name>" in an indirection expression.
type NameIndirection Name

func (n NameIndirection) String() string {
	return fmt.Sprintf(".%s", Name(n))
}

// StarIndirection represents ".*" in an indirection expression.
type StarIndirection string

const (
	qualifiedStar   StarIndirection = ".*"
	unqualifiedStar StarIndirection = "*"
)

func (s StarIndirection) String() string {
	return string(s)
}

// ArrayIndirection represents "[<begin>:<end>]" in an indirection expression.
type ArrayIndirection struct {
	Begin Expr
	End   Expr
}

func (a *ArrayIndirection) String() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	buf.WriteString(a.Begin.String())
	if a.End != nil {
		buf.WriteString(":")
		buf.WriteString(a.End.String())
	}
	buf.WriteString("]")
	return buf.String()
}
