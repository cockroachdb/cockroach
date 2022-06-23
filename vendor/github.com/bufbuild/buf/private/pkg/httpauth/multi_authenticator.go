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

type multiAuthenticator struct {
	authenticators []Authenticator
}

func newMultiAuthenticator(authenticators ...Authenticator) *multiAuthenticator {
	return &multiAuthenticator{
		authenticators: authenticators,
	}
}

func (a *multiAuthenticator) SetAuth(envContainer app.EnvContainer, request *http.Request) (bool, error) {
	switch len(a.authenticators) {
	case 0:
		return false, nil
	case 1:
		return a.authenticators[0].SetAuth(envContainer, request)
	default:
		for _, authenticator := range a.authenticators {
			ok, err := authenticator.SetAuth(envContainer, request)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
}
