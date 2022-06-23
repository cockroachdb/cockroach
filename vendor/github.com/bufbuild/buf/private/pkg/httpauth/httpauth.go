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

package httpauth

import (
	"net/http"

	"github.com/bufbuild/buf/private/pkg/app"
)

// Authenticator adds authentication to request.
//
// This could be instead replaced with a http.RoundTripper workflow, however
// this means we have to use the same http.Client, and we generally pass EnvContainers
// to functions right now, and not building objects with EnvContainers, so this would
// not fit in with the rest of this codebase. This should be re-evaluated though.
type Authenticator interface {
	// SetAuth sets authentication on the request.
	//
	// Returns true if authentication successfully set.
	// Does nothing and returns false if no authentication available for the given request.
	// Does nothing and returns false if the request scheme is not https.
	SetAuth(envContainer app.EnvContainer, request *http.Request) (bool, error)
}

// NewEnvAuthenticator returns a new env Authenticator for the environment.
func NewEnvAuthenticator(usernameKey string, passwordKey string) Authenticator {
	return newEnvAuthenticator(
		usernameKey,
		passwordKey,
	)
}

// NewNetrcAuthenticator returns a new netrc Authenticator.
func NewNetrcAuthenticator() Authenticator {
	return newNetrcAuthenticator()
}

// NewNopAuthenticator returns a new nop Authenticator.
//
// Always returns false and nil.
func NewNopAuthenticator() Authenticator {
	return newNopAuthenticator()
}

// NewMultiAuthenticator returns a new multi Authenticator.
//
// Stops on first matching SetAuth request.
func NewMultiAuthenticator(authenticators ...Authenticator) Authenticator {
	return newMultiAuthenticator(authenticators...)
}
