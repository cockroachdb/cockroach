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
	"errors"
	"net/http"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/netrc"
)

type netrcAuthenticator struct{}

func newNetrcAuthenticator() *netrcAuthenticator {
	return &netrcAuthenticator{}
}

func (a *netrcAuthenticator) SetAuth(envContainer app.EnvContainer, request *http.Request) (bool, error) {
	if request.URL == nil {
		return false, errors.New("malformed request: no url")
	}
	if request.URL.Host == "" {
		return false, errors.New("malformed request: no url host")
	}
	machine, err := netrc.GetMachineForName(envContainer, request.URL.Host)
	if err != nil {
		return false, err
	}
	if machine == nil {
		return false, nil
	}
	return setBasicAuth(
		request,
		machine.Login(),
		machine.Password(),
		"netrc login for host",
		"netrc password for host",
	)
}
