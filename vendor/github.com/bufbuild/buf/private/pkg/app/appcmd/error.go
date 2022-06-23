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

package appcmd

// invalidArgumentError is used to indicate that an error was
// caused by argument validation.
type invalidArgumentError struct {
	message string
}

func newInvalidArgumentError(message string) *invalidArgumentError {
	return &invalidArgumentError{message: message}
}

func (a *invalidArgumentError) Error() string {
	return a.message
}

func (a *invalidArgumentError) Is(err error) bool {
	_, ok := err.(*invalidArgumentError)
	return ok
}
