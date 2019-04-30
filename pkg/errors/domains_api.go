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

// Domain forwards a definition.
type Domain = domains.Domain

// NoDomain forwards a definition.
const NoDomain Domain = domains.NoDomain

// NamedDomain forwards a definition.
var NamedDomain func(domainName string) Domain = domains.NamedDomain

// PackageDomain forwards a definition.
var PackageDomain func() Domain = domains.PackageDomain

// WithDomain forwards a definition.
var WithDomain func(err error, domain Domain) error = domains.WithDomain

// NotInDomain forwards a definition.
var NotInDomain func(err error, domains ...Domain) bool = domains.NotInDomain

// EnsureNotInDomain forwards a definition.
var EnsureNotInDomain func(err error, constructor DomainOverrideFn, forbiddenDomains ...Domain) error = domains.EnsureNotInDomain

// DomainOverrideFn forwards a definition.
type DomainOverrideFn = func(originalDomain Domain, err error) error

// HandledInDomain forwards a definition.
var HandledInDomain func(err error, domain Domain) error = domains.HandledInDomain

// HandledInDomainWithMessage forwards a definition.
var HandledInDomainWithMessage func(err error, domain Domain, msg string) error = domains.HandledInDomainWithMessage

// GetDomain forwards a definition.
var GetDomain func(err error) Domain = domains.GetDomain
