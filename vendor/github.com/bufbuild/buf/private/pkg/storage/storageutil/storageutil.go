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

// Package storageutil provides helpers for storage implementations.
package storageutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/bufbuild/buf/private/pkg/normalpath"
)

// ObjectInfo is an embeddable ObjectInfo.
type ObjectInfo struct {
	path         string
	externalPath string
}

// NewObjectInfo returns a new ObjectInfo.
func NewObjectInfo(
	path string,
	externalPath string,
) ObjectInfo {
	return ObjectInfo{
		path:         path,
		externalPath: externalPath,
	}
}

// Path implements ObjectInfo.
func (o ObjectInfo) Path() string {
	return o.path
}

// ExternalPath implements ObjectInfo.
func (o ObjectInfo) ExternalPath() string {
	return o.externalPath
}

// ValidatePath validates a path.
func ValidatePath(path string) (string, error) {
	path, err := normalpath.NormalizeAndValidate(path)
	if err != nil {
		return "", err
	}
	if path == "." {
		return "", errors.New("cannot use root")
	}
	return path, nil
}

// ValidatePrefix validates a prefix.
func ValidatePrefix(prefix string) (string, error) {
	return normalpath.NormalizeAndValidate(prefix)
}

// WalkChecker does validation for every step of a walk.
type WalkChecker interface {
	Check(ctx context.Context) error
}

// NewWalkChecker returns a new WalkChecker.
func NewWalkChecker() WalkChecker {
	return &walkChecker{}
}

type walkChecker struct {
	count int
}

func (w *walkChecker) Check(ctx context.Context) error {
	w.count++
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err == context.DeadlineExceeded {
			return fmt.Errorf("timed out after %d files: %v", w.count, err)
		}
		return err
	default:
		return nil
	}
}
