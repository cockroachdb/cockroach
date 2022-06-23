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
	"fmt"
	"net/http"
)

func setBasicAuth(
	request *http.Request,
	username string,
	password string,
	usernameKey string,
	passwordKey string,
) (bool, error) {
	if request.URL == nil {
		return false, errors.New("malformed request: no url")
	}
	if request.URL.Scheme == "" {
		return false, errors.New("malformed request: no url scheme")
	}
	if request.URL.Scheme != "https" {
		return false, nil
	}
	if username != "" && password != "" {
		request.SetBasicAuth(username, password)
		return true, nil
	}
	if username == "" && password == "" {
		return false, nil
	}
	if password == "" {
		return false, fmt.Errorf("%s set but %s not set", usernameKey, passwordKey)
	}
	return false, fmt.Errorf("%s set but %s not set", passwordKey, usernameKey)
}
