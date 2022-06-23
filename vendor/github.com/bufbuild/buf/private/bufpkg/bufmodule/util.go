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

package bufmodule

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	modulev1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/module/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/storage"
	"go.uber.org/multierr"
)

// sortFileInfos sorts the FileInfos.
func sortFileInfos(fileInfos []FileInfo) {
	if len(fileInfos) == 0 {
		return
	}
	sort.Slice(
		fileInfos,
		func(i int, j int) bool {
			return fileInfos[i].Path() < fileInfos[j].Path()
		},
	)
}

// parseModuleReferenceComponents parses and returns the remote, owner, repository,
// and ref (branch or commit) from the given path.
func parseModuleReferenceComponents(path string) (remote string, owner string, repository string, ref string, err error) {
	remote, owner, rest, err := parseModuleIdentityComponents(path)
	if err != nil {
		return "", "", "", "", newInvalidModuleReferenceStringError(path)
	}
	restSplit := strings.Split(rest, ":")
	repository = strings.TrimSpace(restSplit[0])
	if len(restSplit) == 1 {
		return remote, owner, repository, "", nil
	}
	if len(restSplit) == 2 {
		ref := strings.TrimSpace(restSplit[1])
		if ref == "" {
			return "", "", "", "", newInvalidModuleReferenceStringError(path)
		}
		return remote, owner, repository, ref, nil
	}
	return "", "", "", "", newInvalidModuleReferenceStringError(path)
}

func parseModuleIdentityComponents(path string) (remote string, owner string, repository string, err error) {
	slashSplit := strings.Split(path, "/")
	if len(slashSplit) != 3 {
		return "", "", "", newInvalidModuleIdentityStringError(path)
	}
	remote = strings.TrimSpace(slashSplit[0])
	if remote == "" {
		return "", "", "", newInvalidModuleIdentityStringError(path)
	}
	owner = strings.TrimSpace(slashSplit[1])
	if owner == "" {
		return "", "", "", newInvalidModuleIdentityStringError(path)
	}
	repository = strings.TrimSpace(slashSplit[2])
	if repository == "" {
		return "", "", "", newInvalidModuleIdentityStringError(path)
	}
	return remote, owner, repository, nil
}

func modulePinLess(a ModulePin, b ModulePin) bool {
	return modulePinCompareTo(a, b) < 0
}

// return -1 if less
// return 1 if greater
// return 0 if equal
func modulePinCompareTo(a ModulePin, b ModulePin) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil && b != nil {
		return -1
	}
	if a != nil && b == nil {
		return 1
	}
	if a.Remote() < b.Remote() {
		return -1
	}
	if a.Remote() > b.Remote() {
		return 1
	}
	if a.Owner() < b.Owner() {
		return -1
	}
	if a.Owner() > b.Owner() {
		return 1
	}
	if a.Repository() < b.Repository() {
		return -1
	}
	if a.Repository() > b.Repository() {
		return 1
	}
	if a.Branch() < b.Branch() {
		return -1
	}
	if a.Branch() > b.Branch() {
		return 1
	}
	if a.Commit() < b.Commit() {
		return -1
	}
	if a.Commit() > b.Commit() {
		return 1
	}
	if a.Digest() < b.Digest() {
		return -1
	}
	if a.Digest() > b.Digest() {
		return 1
	}
	if a.CreateTime().Before(b.CreateTime()) {
		return -1
	}
	if a.CreateTime().After(b.CreateTime()) {
		return 1
	}
	return 0
}

func copyModulePinsSortedByOnlyCommit(modulePins []ModulePin) []ModulePin {
	s := make([]ModulePin, len(modulePins))
	copy(s, modulePins)
	sort.Slice(s, func(i, j int) bool {
		return modulePinLessOnlyCommit(s[i], s[j])
	})
	return s
}

func modulePinLessOnlyCommit(a ModulePin, b ModulePin) bool {
	return modulePinCompareToOnlyCommit(a, b) < 0
}

// return -1 if less
// return 1 if greater
// return 0 if equal
func modulePinCompareToOnlyCommit(a ModulePin, b ModulePin) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil && b != nil {
		return -1
	}
	if a != nil && b == nil {
		return 1
	}
	if a.Remote() < b.Remote() {
		return -1
	}
	if a.Remote() > b.Remote() {
		return 1
	}
	if a.Owner() < b.Owner() {
		return -1
	}
	if a.Owner() > b.Owner() {
		return 1
	}
	if a.Repository() < b.Repository() {
		return -1
	}
	if a.Repository() > b.Repository() {
		return 1
	}
	if a.Commit() < b.Commit() {
		return -1
	}
	if a.Commit() > b.Commit() {
		return 1
	}
	return 0
}

func putModuleFileToBucket(ctx context.Context, module Module, path string, writeBucket storage.WriteBucket) (retErr error) {
	moduleFile, err := module.GetModuleFile(ctx, path)
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, moduleFile.Close())
	}()
	var copyOptions []storage.CopyOption
	if writeBucket.SetExternalPathSupported() {
		copyOptions = append(copyOptions, storage.CopyWithExternalPaths())
	}
	return storage.CopyReadObject(ctx, writeBucket, moduleFile, copyOptions...)
}

func moduleFileToProto(ctx context.Context, module Module, path string) (_ *modulev1alpha1.ModuleFile, retErr error) {
	protoModuleFile := &modulev1alpha1.ModuleFile{
		Path: path,
	}
	moduleFile, err := module.GetModuleFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = multierr.Append(retErr, moduleFile.Close())
	}()
	protoModuleFile.Content, err = io.ReadAll(moduleFile)
	if err != nil {
		return nil, err
	}
	return protoModuleFile, nil
}

func getDocumentationForBucket(
	ctx context.Context,
	readBucket storage.ReadBucket,
) (string, error) {
	documentationData, err := storage.ReadPath(ctx, readBucket, DocumentationFilePath)
	if err != nil {
		if storage.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(documentationData), nil
}

func newInvalidModuleOwnerStringError(s string) error {
	return fmt.Errorf("module owner %q is invalid: must be in the form remote/owner", s)
}

func newInvalidModuleIdentityStringError(s string) error {
	return fmt.Errorf("module identity %q is invalid: must be in the form remote/owner/repository", s)
}

func newInvalidModuleReferenceStringError(s string) error {
	return fmt.Errorf("module reference %q is invalid: must be in the form remote/owner/repository:branch or remote/owner/repository:commit", s)
}
