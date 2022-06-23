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

package bufcli

import (
	"context"
	"errors"
	"fmt"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/rpc"
)

const (
	// ExitCodeFileAnnotation is the exit code used when we print file annotations.
	//
	// We use a different exit code to be able to distinguish user-parsable errors from
	// system errors.
	ExitCodeFileAnnotation = 100
)

var (
	// ErrNoModuleName is used when the user does not specify a module name in their configuration file.
	ErrNoModuleName = errors.New(`please specify a module name in your configuration file with the "name" key`)

	// ErrNoConfigFile is used when the user tries to execute a command without a configuration file.
	ErrNoConfigFile = errors.New(`please define a configuration file in the current directory; you can create one by running "buf mod init"`)

	// ErrFileAnnotation is used when we print file annotations and want to return an error.
	//
	// The app package works on the concept that an error results in a non-zero exit
	// code, and we already print the messages with PrintFileAnnotations, so we do
	// not want to print any additional error message.
	//
	// We also exit with 100 to be able to distinguish user-parsable errors from
	// system errors.
	ErrFileAnnotation = app.NewError(ExitCodeFileAnnotation, "")
)

// errInternal is returned when the user encounters an unexpected internal buf error.
type errInternal struct {
	cause error
}

// NewInternalError represents an internal error encountered by the buf CLI.
// These errors should not happen and therefore warrant a bug report.
func NewInternalError(err error) error {
	if isInternalError(err) {
		return err
	}
	return &errInternal{cause: err}
}

// isInternalError returns whether the error provided, or
// any error wrapped by that error, is an internal error.
func isInternalError(err error) bool {
	return errors.Is(err, &errInternal{})
}

func (e *errInternal) Error() string {
	message := "it looks like you have found a bug in buf. " +
		"Please file an issue at https://github.com/bufbuild/buf/issues/ " +
		"and provide the command you ran"
	if e.cause == nil {
		return message
	}
	return message + ", as well as the following message: " + e.cause.Error()
}

// Is implements errors.Is for errInternal.
func (e *errInternal) Is(err error) bool {
	_, ok := err.(*errInternal)
	return ok
}

// NewErrorInterceptor returns a CLI interceptor that wraps Buf CLI errors.
func NewErrorInterceptor() appflag.Interceptor {
	return func(next func(context.Context, appflag.Container) error) func(context.Context, appflag.Container) error {
		return func(ctx context.Context, container appflag.Container) error {
			return wrapError(next(ctx, container))
		}
	}
}

// NewModuleRefError is used when the client fails to parse a module ref.
func NewModuleRefError(moduleRef string) error {
	return fmt.Errorf("could not parse %q as a module; please verify this is a valid reference", moduleRef)
}

// NewTooManyEmptyAnswersError is used when the user does not answer a prompt in
// the given number of attempts.
func NewTooManyEmptyAnswersError(attempts int) error {
	return fmt.Errorf("did not receive an answer in %d attempts", attempts)
}

// NewOrganizationNameAlreadyExistsError informs the user that an organization with
// that name already exists.
func NewOrganizationNameAlreadyExistsError(name string) error {
	return fmt.Errorf("an organization named %q already exists", name)
}

// NewRepositoryNameAlreadyExistsError informs the user that a repository
// with that name already exists.
func NewRepositoryNameAlreadyExistsError(name string) error {
	return fmt.Errorf("a repository named %q already exists", name)
}

// NewBranchOrTagNameAlreadyExistsError informs the user that a branch
// or tag with that name already exists.
func NewBranchOrTagNameAlreadyExistsError(name string) error {
	return fmt.Errorf("a branch or tag named %q already exists", name)
}

// NewOrganizationNotFoundError informs the user that an organization with
// that name does not exist.
func NewOrganizationNotFoundError(name string) error {
	return fmt.Errorf(`an organization named %q does not exist, use "buf beta registry organization create" to create one`, name)
}

// NewRepositoryNotFoundError informs the user that a repository with
// that name does not exist.
func NewRepositoryNotFoundError(name string) error {
	return fmt.Errorf(`a repository named %q does not exist, use "buf beta registry repository create" to create one`, name)
}

// NewModuleReferenceNotFoundError informs the user that a module
// reference does not exist.
func NewModuleReferenceNotFoundError(reference bufmodule.ModuleReference) error {
	return fmt.Errorf("%q does not exist", reference)
}

// NewTokenNotFoundError informs the user that a token with
// that identifier does not exist.
func NewTokenNotFoundError(tokenID string) error {
	return fmt.Errorf("a token with ID %q does not exist", tokenID)
}

func NewUnimplementedRemoteError(err error, remote string, moduleIdentity string) error {
	return fmt.Errorf("%w. Are you sure %q (derived from module name %q) is a Buf Schema Registry?", err, remote, moduleIdentity)
}

// NewPluginNotFoundError informs the user that a plugin with
// that owner and name does not exist.
func NewPluginNotFoundError(owner string, name string) error {
	return fmt.Errorf("the plugin %s/%s does not exist", owner, name)
}

// NewTemplateNotFoundError informs the user that a template with
// that owner and name does not exist.
func NewTemplateNotFoundError(owner string, name string) error {
	return fmt.Errorf("the template %s/%s does not exist", owner, name)
}

// wrapError is used when a CLI command fails, regardless of its error code.
// Note that this function will wrap the error so that the underlying error
// can be recovered via 'errors.Is'.
func wrapError(err error) error {
	if err == nil || (err.Error() == "" && !rpc.IsError(err)) {
		// If the error is nil or empty and not an rpc error, we return it as-is.
		// This is especially relevant for commands like lint and breaking.
		return err
	}
	rpcCode := rpc.GetErrorCode(err)
	switch {
	case rpcCode == rpc.ErrorCodeUnauthenticated, isEmptyUnknownError(err):
		return errors.New(`Failure: you are not authenticated. Create a new entry in your netrc, using a Buf API Key as the password. For details, visit https://docs.buf.build/bsr/authentication`)
	case rpcCode == rpc.ErrorCodeUnavailable:
		return fmt.Errorf(`Failure: the server hosted at that remote is unavailable: %w.`, err)
	}
	return fmt.Errorf("Failure: %w.", err)
}

// isEmptyUnknownError returns true if the given
// error is non-nil, but has an empty message
// and an unknown error code.
//
// This is relevant for errors returned by
// envoyauthd when the client does not provide
// an authentication header.
func isEmptyUnknownError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "" && rpc.GetErrorCode(err) == rpc.ErrorCodeUnknown
}
