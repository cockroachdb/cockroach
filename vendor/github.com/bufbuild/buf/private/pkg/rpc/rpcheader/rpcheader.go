// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package rpcheader is a helper package.
//
// This is not internal as we need to share it across multiple directory structures within Buf.
package rpcheader

import "strings"

// KeyPrefix is the prefix of both http and grpc headers
// set with rpc packages.
const KeyPrefix = "rpc-"

// StripHeaderPrefixes strips the KeyPrefix from any of the header keys.
func StripHeaderPrefixes(headers map[string]string) map[string]string {
	out := make(map[string]string)
	for key, value := range headers {
		key = strings.ToLower(key)
		if !strings.HasPrefix(key, KeyPrefix) {
			out[key] = value
		}
		trimmedKey := strings.TrimPrefix(key, KeyPrefix)
		if trimmedKey == "" {
			out[key] = value
		}
		out[trimmedKey] = value
	}
	return out
}
