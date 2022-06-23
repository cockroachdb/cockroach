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

// Package osextended provides os utilities.
package osextended

import (
	"errors"
	"os"
	"sync"
)

var (
	workDirPath    string
	workDirPathErr error
	once           sync.Once

	errOSGetwdEmpty = errors.New("os.Getwd returned empty and no error")
)

// Getwd replaces os.Getwd and caches the result.
func Getwd() (string, error) {
	once.Do(func() {
		workDirPath, workDirPathErr = getwdUncached()
	})
	return workDirPath, workDirPathErr
}

func getwdUncached() (string, error) {
	currentWorkDirPath, currentWorkDirPathErr := os.Getwd()
	if currentWorkDirPath == "" && currentWorkDirPathErr == nil {
		return "", errOSGetwdEmpty
	}
	return currentWorkDirPath, currentWorkDirPathErr
}
