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

package buftransport

import (
	"fmt"
	"strings"

	"github.com/bufbuild/buf/private/pkg/app"
)

const (
	apiSubdomain = "api"

	// TODO: change to based on "use"
	disableAPISubdomainEnvKey = "BUF_DISABLE_API_SUBDOMAIN"
	// TODO: change to based on "use"
	transportEnvKey = "BUF_TRANSPORT"
)

// IsAPISubdomainEnabled returns true if the container says to use the API subdomain.
func IsAPISubdomainEnabled(container app.EnvContainer) bool {
	return strings.TrimSpace(strings.ToLower(container.Env(disableAPISubdomainEnvKey))) == ""
}

// SetDisableAPISubdomain sets the environment map to disable the API subdomain.
func SetDisableAPISubdomain(env map[string]string) {
	env[disableAPISubdomainEnvKey] = "disable"
}

// UseGRPC returns true if the container says to use grpc.
func UseGRPC(container app.EnvContainer) (bool, error) {
	switch value := strings.TrimSpace(strings.ToLower(container.Env(transportEnvKey))); value {
	// default
	case "", "grpc":
		return true, nil
	case "twirp":
		return false, nil
	default:
		return false, fmt.Errorf("unknown value for %s: %s", transportEnvKey, value)
	}
}

// SetUseGRPC sets the environment map to use grpc based on the value.
func SetUseGRPC(env map[string]string, useGRPC bool) {
	if useGRPC {
		env[transportEnvKey] = "grpc"
	} else {
		env[transportEnvKey] = "twirp"
	}
}

// PrependAPISubdomain prepends the API subdomain to the given address.
func PrependAPISubdomain(address string) string {
	return apiSubdomain + "." + address
}
