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

package app

import (
	"errors"
	"strings"
)

type envContainer struct {
	variables map[string]string
}

func newEnvContainer(m map[string]string) *envContainer {
	variables := make(map[string]string)
	for key, value := range m {
		if value != "" {
			variables[key] = value
		}
	}
	return &envContainer{
		variables: variables,
	}
}

func newEnvContainerForEnviron(environ []string) (*envContainer, error) {
	variables := make(map[string]string, len(environ))
	for _, elem := range environ {
		if !strings.ContainsRune(elem, '=') {
			// Do not print out as we don't want to mistakenly leak a secure environment variable
			return nil, errors.New("environment variable does not contain =")
		}
		split := strings.SplitN(elem, "=", 2)
		if len(split) != 2 {
			// Do not print out as we don't want to mistakenly leak a secure environment variable
			return nil, errors.New("unknown environment split")
		}
		if split[1] != "" {
			variables[split[0]] = split[1]
		}
	}
	return &envContainer{
		variables: variables,
	}, nil
}

func (e *envContainer) Env(key string) string {
	return e.variables[key]
}

func (e *envContainer) ForEachEnv(f func(string, string)) {
	for key, value := range e.variables {
		// This should be done anyways but just to make sure
		if value != "" {
			f(key, value)
		}
	}
}
