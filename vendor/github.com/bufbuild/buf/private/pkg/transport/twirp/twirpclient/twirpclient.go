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

// Package twirpclient provides twirp-specific building blocks on top of httpclient
// to provide consistent twirp usage.
package twirpclient

import (
	"github.com/bufbuild/buf/private/pkg/rpc/rpctwirp"
	"github.com/twitchtv/twirp"
)

// NewClientOptions returns consistent twirp.ClientOptions.
func NewClientOptions() []twirp.ClientOption {
	return []twirp.ClientOption{
		twirp.WithClientInterceptors(
			rpctwirp.NewClientInterceptor(),
		),
		twirp.WithClientPathPrefix(""),
	}
}
