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

const (
	// Endpoint is the URL path prefix which accepts incoming
	// HTTP requests for the SQL API.
	Endpoint = "/sql/"
)

// A Call is a pending database API call.
type Call struct {
	Args  *Request  // The argument to the command
	Reply *Response // The reply from the command
}

// Header returns the request header.
func (r *RequestHeader) Header() *RequestHeader {
	return r
}

// Method returns the method.
func (*Request) Method() Method {
	return Execute
}

// CreateReply creates an empty response for the request.
func (*Request) CreateReply() *Response {
	return &Response{}
}

// Header returns the response header.
func (r *ResponseHeader) Header() *ResponseHeader {
	return r
}
