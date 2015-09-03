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
