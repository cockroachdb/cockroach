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
	"fmt"
)

type appError struct {
	exitCode int
	message  string
}

func newAppError(exitCode int, message string) *appError {
	if exitCode == 0 {
		message = fmt.Sprintf(
			"got invalid exit code %d when constructing error (original message was %q)",
			exitCode,
			message,
		)
		exitCode = 1
	}
	return &appError{
		exitCode: exitCode,
		message:  message,
	}
}

func (e *appError) Error() string {
	return e.message
}

func printError(container StderrContainer, err error) {
	if errString := err.Error(); errString != "" {
		_, _ = fmt.Fprintln(container.Stderr(), errString)
	}
}
