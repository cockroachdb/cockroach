// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/domains"

// Domain is the type of a domain annotation.
type Domain = domains.Domain

// NoDomain is the domain of errors that don't originate
// from a barrier.
const NoDomain Domain = domains.NoDomain

// NamedDomain returns an error domain identified by the given string.
var NamedDomain func(domainName string) Domain = domains.NamedDomain

// PackageDomain returns an error domain that represents the
// package of its caller.
var PackageDomain func() Domain = domains.PackageDomain

// WithDomain wraps an error so that it appears to come from the given domain.
var WithDomain func(err error, domain Domain) error = domains.WithDomain

// NotInDomain returns true if and only if the error's
// domain is not one of the specified domains.
var NotInDomain func(err error, domains ...Domain) bool = domains.NotInDomain

// EnsureNotInDomain checks whether the error is in the given domain(s).
// If it is, the given constructor if provided is called to construct
// an alternate error. If no error constructor is provided,
// a new barrier is constructed automatically using the first
// provided domain as new domain. The original error message
// is preserved.
var EnsureNotInDomain func(err error, constructor DomainOverrideFn, forbiddenDomains ...Domain) error = domains.EnsureNotInDomain

// DomainOverrideFn is the callback type suitable for EnsureNotInDomain.
type DomainOverrideFn = func(originalDomain Domain, err error) error

// HandledInDomain creates an error in the given domain and retains
// the details of the given original error as context for
// debugging. The original error is hidden and does not become a
// "cause" for the new error. The original's error _message_
// is preserved.
//
// This combines Handled() and WithDomain().
var HandledInDomain func(err error, domain Domain) error = domains.HandledInDomain

// HandledInDomainWithMessage combines HandledWithMessage() and WithDomain().
var HandledInDomainWithMessage func(err error, domain Domain, msg string) error = domains.HandledInDomainWithMessage

// GetDomain extracts the domain of the given error, or NoDomain if
// the error's cause does not have a domain annotation.
var GetDomain func(err error) Domain = domains.GetDomain
