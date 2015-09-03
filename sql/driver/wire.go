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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/parser"
)

const (
	// Endpoint is the URL path prefix which accepts incoming
	// HTTP requests for the SQL API.
	Endpoint = "/sql/"
)

func (d Datum) String() string {
	val := d.GetValue()

	if val == nil {
		return "NULL"
	}

	switch t := val.(type) {
	case *bool:
		return strconv.FormatBool(*t)
	case *int64:
		return strconv.FormatInt(*t, 10)
	case *float64:
		return strconv.FormatFloat(*t, 'g', -1, 64)
	case []byte:
		return string(t)
	case *string:
		return *t
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}

// Method returns the method.
func (Request) Method() Method {
	return Execute
}

// CreateReply creates an empty response for the request.
func (Request) CreateReply() Response {
	return Response{}
}

// GetParameters returns the Params slice as a `parameters`.
func (r Request) GetParameters() Parameters {
	return Parameters(r.Params)
}

// Parameters implements the parser.Args interface.
type Parameters []Datum

// Arg implements the parser.Args interface.
func (p Parameters) Arg(name string) (parser.Datum, bool) {
	if len(name) == 0 {
		// This shouldn't happen unless the parser let through an invalid parameter
		// specification.
		panic(fmt.Sprintf("invalid empty parameter name"))
	}
	if ch := name[0]; ch < '0' || ch > '9' {
		// TODO(pmattis): Add support for named parameters (vs the numbered
		// parameter support below).
		return nil, false
	}
	i, err := strconv.ParseInt(name, 10, 0)
	if err != nil {
		return nil, false
	}
	if i < 1 || int(i) > len(p) {
		return nil, false
	}
	arg := p[i-1].GetValue()
	if arg == nil {
		return parser.DNull, true
	}
	switch t := arg.(type) {
	case *bool:
		return parser.DBool(*t), true
	case *int64:
		return parser.DInt(*t), true
	case *float64:
		return parser.DFloat(*t), true
	case []byte:
		return parser.DString(t), true
	case *string:
		return parser.DString(*t), true
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}
