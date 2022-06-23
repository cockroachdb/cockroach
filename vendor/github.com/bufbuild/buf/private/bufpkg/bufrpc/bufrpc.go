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

// Package bufrpc provides buf-specific rpc functionality.
package bufrpc

import (
	"context"

	"github.com/bufbuild/buf/private/pkg/rpc"
)

const (
	// DefaultRemote is the default remote if none can be inferred from a module name.
	DefaultRemote = "buf.build"
	// cliVersionHeaderName is the name of the header carrying the buf CLI version.
	cliVersionHeaderName = "buf-version"
)

// WithOutgoingCLIVersionHeader attaches the given CLI version to the context for rpc.
func WithOutgoingCLIVersionHeader(ctx context.Context, cliVersion string) context.Context {
	return rpc.WithOutgoingHeader(ctx, cliVersionHeaderName, cliVersion)
}

// GetIncomingCLIVersionHeader gets the CLI version from the context for rpc.
//
// Returns empty if no CLI version is attached to the context.
func GetIncomingCLIVersionHeader(ctx context.Context) string {
	return rpc.GetIncomingHeader(ctx, cliVersionHeaderName)
}
