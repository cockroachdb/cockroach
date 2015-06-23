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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sqlwire

import (
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// Endpoint is the URL path prefix which accepts incoming
	// HTTP requests for the SQL API.
	Endpoint = "/sql/"
)

// Request is an interface for RPC requests.
type Request interface {
	gogoproto.Message
	Header() *SQLRequestHeader
	// Method returns the request method.
	Method() Method
	// CreateReply creates a new response object.
	CreateReply() Response
}

// Response is an interface for RPC responses.
type Response interface {
	gogoproto.Message
	// Header returns the response header.
	Header() *SQLResponseHeader
}

// Header returns the request header.
func (r *SQLRequest) Header() *SQLRequestHeader {
	return r.Header()
}

// Method returns the method.
func (*SQLRequest) Method() Method {
	return Execute
}

// CreateReply creates an empty response for the request.
func (*SQLRequest) CreateReply() Response {
	return &SQLResponse{}
}

// Header returns the response header.
func (r *SQLResponse) Header() *SQLResponseHeader {
	return r.Header()
}
