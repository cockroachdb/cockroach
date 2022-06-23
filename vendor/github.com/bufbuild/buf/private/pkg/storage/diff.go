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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bufbuild/buf/private/pkg/diff"
)

// DiffOption is an option for Diff.
type DiffOption func(*diffOptions)

// DiffWithSuppressCommands returns a new DiffOption that suppresses printing of commands.
func DiffWithSuppressCommands() DiffOption {
	return func(diffOptions *diffOptions) {
		diffOptions.suppressCommands = true
	}
}

// DiffWithSuppressCommands returns a new DiffOption that suppresses printing of timestamps.
func DiffWithSuppressTimestamps() DiffOption {
	return func(diffOptions *diffOptions) {
		diffOptions.suppressTimestamps = true
	}
}

// DiffWithExternalPaths returns a new DiffOption that prints diffs with external paths
// instead of paths.
func DiffWithExternalPaths() DiffOption {
	return func(diffOptions *diffOptions) {
		diffOptions.externalPaths = true
	}
}

// DiffWithExternalPathPrefixes returns a new DiffOption that sets the external path prefixes for the buckets.
//
// If a file is in one bucket but not the other, it will be assumed that the file begins
// with the given prefix, and this prefix should be substituted for the other prefix.
//
// For example, if diffing the directories "test/a" and "test/b", use "test/a/" and "test/b/",
// and a file that is in one with path "test/a/foo.txt" will be shown as not
// existing as "test/b/foo.txt" in two.
//
// Note that the prefixes are directly concatenated, so "/" should be included generally.
//
// This option has no effect if DiffWithExternalPaths is not set.
// This option is not required if the prefixes are equal.
func DiffWithExternalPathPrefixes(
	oneExternalPathPrefix string,
	twoExternalPathPrefix string,
) DiffOption {
	return func(diffOptions *diffOptions) {
		if oneExternalPathPrefix != twoExternalPathPrefix {
			// we don't know if external paths are file paths or not
			// so we just operate on pure string-prefix paths
			// this comes up with for example s3://
			diffOptions.oneExternalPathPrefix = oneExternalPathPrefix
			diffOptions.twoExternalPathPrefix = twoExternalPathPrefix
		}
	}
}

// DiffBytes does a diff of the ReadBuckets.
func DiffBytes(
	ctx context.Context,
	one ReadBucket,
	two ReadBucket,
	options ...DiffOption,
) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	if err := Diff(ctx, buffer, one, two, options...); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Diff writes a diff of the ReadBuckets to the Writer.
func Diff(
	ctx context.Context,
	writer io.Writer,
	one ReadBucket,
	two ReadBucket,
	options ...DiffOption,
) error {
	diffOptions := newDiffOptions()
	for _, option := range options {
		option(diffOptions)
	}
	externalPaths := diffOptions.externalPaths
	oneExternalPathPrefix := diffOptions.oneExternalPathPrefix
	twoExternalPathPrefix := diffOptions.twoExternalPathPrefix

	oneObjectInfos, err := allObjectInfos(ctx, one, "")
	if err != nil {
		return err
	}
	twoObjectInfos, err := allObjectInfos(ctx, two, "")
	if err != nil {
		return err
	}
	sortObjectInfos(oneObjectInfos)
	sortObjectInfos(twoObjectInfos)
	onePathToObjectInfo := pathToObjectInfo(oneObjectInfos)
	twoPathToObjectInfo := pathToObjectInfo(twoObjectInfos)

	for _, oneObjectInfo := range oneObjectInfos {
		path := oneObjectInfo.Path()
		oneDiffPath, err := getDiffPathForObjectInfo(
			oneObjectInfo,
			externalPaths,
			oneExternalPathPrefix,
		)
		if err != nil {
			return err
		}
		oneData, err := ReadPath(ctx, one, path)
		if err != nil {
			return err
		}
		var twoData []byte
		var twoDiffPath string
		if twoObjectInfo, ok := twoPathToObjectInfo[path]; ok {
			twoData, err = ReadPath(ctx, two, path)
			if err != nil {
				return err
			}
			twoDiffPath, err = getDiffPathForObjectInfo(
				twoObjectInfo,
				externalPaths,
				twoExternalPathPrefix,
			)
			if err != nil {
				return err
			}
		} else {
			twoDiffPath, err = getDiffPathForNotFound(
				oneObjectInfo,
				externalPaths,
				oneExternalPathPrefix,
				twoExternalPathPrefix,
			)
			if err != nil {
				return err
			}
		}
		diffData, err := diff.Diff(
			ctx,
			oneData,
			twoData,
			oneDiffPath,
			twoDiffPath,
			diffOptions.toDiffPackageOptions()...,
		)
		if err != nil {
			return err
		}
		if len(diffData) > 0 {
			if _, err := writer.Write(diffData); err != nil {
				return err
			}
		}
	}
	for _, twoObjectInfo := range twoObjectInfos {
		path := twoObjectInfo.Path()
		if _, ok := onePathToObjectInfo[path]; !ok {
			twoData, err := ReadPath(ctx, two, path)
			if err != nil {
				return err
			}
			oneDiffPath, err := getDiffPathForNotFound(
				twoObjectInfo,
				externalPaths,
				twoExternalPathPrefix,
				oneExternalPathPrefix,
			)
			if err != nil {
				return err
			}
			twoDiffPath, err := getDiffPathForObjectInfo(
				twoObjectInfo,
				externalPaths,
				twoExternalPathPrefix,
			)
			if err != nil {
				return err
			}
			diffData, err := diff.Diff(
				ctx,
				nil,
				twoData,
				oneDiffPath,
				twoDiffPath,
				diffOptions.toDiffPackageOptions()...,
			)
			if err != nil {
				return err
			}
			if len(diffData) > 0 {
				if _, err := writer.Write(diffData); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getDiffPathForObjectInfo(
	objectInfo ObjectInfo,
	externalPaths bool,
	externalPathPrefix string,
) (string, error) {
	if !externalPaths {
		return objectInfo.Path(), nil
	}
	externalPath := objectInfo.ExternalPath()
	if externalPathPrefix == "" {
		return externalPath, nil
	}
	if !strings.HasPrefix(externalPath, externalPathPrefix) {
		return "", fmt.Errorf("diff: expected %s to have prefix %s", externalPath, externalPathPrefix)
	}
	return externalPath, nil
}

func getDiffPathForNotFound(
	foundObjectInfo ObjectInfo,
	externalPaths bool,
	foundExternalPathPrefix string,
	notFoundExternalPathPrefix string,
) (string, error) {
	if !externalPaths {
		return foundObjectInfo.Path(), nil
	}
	externalPath := foundObjectInfo.ExternalPath()
	switch {
	case foundExternalPathPrefix == "" && notFoundExternalPathPrefix == "":
		// no prefix, just return external path
		return externalPath, nil
	case foundExternalPathPrefix == "" && notFoundExternalPathPrefix != "":
		// the not-found side has a prefix, append the external path to this prefix, and we're done
		return notFoundExternalPathPrefix + externalPath, nil
	default:
		//foundExternalPathPrefix != "" && notFoundExternalPathPrefix == ""
		//foundExternalPathPrefix != "" && notFoundExternalPathPrefix != ""
		if !strings.HasPrefix(externalPath, foundExternalPathPrefix) {
			return "", fmt.Errorf("diff: expected %s to have prefix %s", externalPath, foundExternalPathPrefix)
		}
		return notFoundExternalPathPrefix + strings.TrimPrefix(externalPath, foundExternalPathPrefix), nil
	}
}

type diffOptions struct {
	suppressCommands      bool
	suppressTimestamps    bool
	externalPaths         bool
	oneExternalPathPrefix string
	twoExternalPathPrefix string
}

func newDiffOptions() *diffOptions {
	return &diffOptions{}
}

func (d *diffOptions) toDiffPackageOptions() []diff.DiffOption {
	var diffPackageOptions []diff.DiffOption
	if d.suppressCommands {
		diffPackageOptions = append(diffPackageOptions, diff.DiffWithSuppressCommands())
	}
	if d.suppressTimestamps {
		diffPackageOptions = append(diffPackageOptions, diff.DiffWithSuppressTimestamps())
	}
	return diffPackageOptions
}
