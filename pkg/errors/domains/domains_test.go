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

package domains_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/domains"
	"github.com/cockroachdb/cockroach/pkg/errors/domains/internal"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// This test demonstrates that simple errors using the standard Go
// types have no domain.
func TestNoDomain(t *testing.T) {
	err := errors.New("hello")

	tt := testutils.T{T: t}

	tt.CheckEqual(domains.GetDomain(err), domains.NoDomain)
	tt.Check(!domains.NotInDomain(err, domains.NoDomain))
}

// This test demonstrates how it is possible to create a custom domain
// from an arbitrary string.
func TestNamedDomain(t *testing.T) {
	myDomain := domains.NamedDomain("here")
	err := errors.New("hello")
	err = domains.WithDomain(err, myDomain)

	tt := testutils.T{T: t}

	tt.CheckEqual(domains.GetDomain(err), myDomain)
	tt.Check(!domains.NotInDomain(err, myDomain))
	tt.Check(domains.NotInDomain(err, domains.NoDomain))
}

// This test demonstrates that the domain information is visible
// "through" layers of wrapping.
func TestWrappedDomain(t *testing.T) {
	myDomain := domains.NamedDomain("here")
	err := errors.New("hello")
	err = domains.WithDomain(err, myDomain)
	err = errors.Wrap(err, "world")

	tt := testutils.T{T: t}

	tt.Check(!domains.NotInDomain(err, myDomain))
}

// This test demonstrates how it is possible to leave the domain
// implicit, in which case a domain is computed automatically based on
// which package instantiates the error.
func TestPackageDomain(t *testing.T) {
	otherErr := internal.NewError("hello")

	// Then errors fall into the local "package domain" where the error
	// was instantiated.
	otherDomain := internal.ThisDomain

	tt := testutils.T{T: t}

	tt.CheckEqual(domains.GetDomain(otherErr), otherDomain)
	tt.Check(!domains.NotInDomain(otherErr, otherDomain))
	tt.Check(domains.NotInDomain(otherErr, domains.NoDomain))

	hereDomain := domains.PackageDomain()

	tt.Check(hereDomain != otherDomain)

	hereErr := domains.New("hello")

	tt.CheckEqual(domains.GetDomain(hereErr), hereDomain)
	tt.Check(!domains.NotInDomain(hereErr, hereDomain))
	tt.Check(domains.NotInDomain(hereErr, domains.NoDomain))
}

// This test demonstrates how the original domain becomes invisible
// via WithDomain(), but the original error remains visible as cause.
func TestWithDomain(t *testing.T) {
	origErr := domains.New("hello")
	t.Logf("origErr: %# v", pretty.Formatter(origErr))
	origDomain := domains.GetDomain(origErr)

	otherDomain := domains.NamedDomain("woo")
	err := domains.WithDomain(origErr, otherDomain)
	t.Logf("err: %# v", pretty.Formatter(err))

	tt := testutils.T{T: t}

	// The original domain becomes invisible.
	tt.Check(domains.NotInDomain(err, origDomain))

	// The cause remains visible.
	tt.Check(markers.Is(err, origErr))

	// Moreover, the error message is preserved fully.
	tt.CheckEqual(err.Error(), "hello")
}

// This test demonstrates how the original domain becomes invisible
// via HandledInDomain(), and even the original error becomes invisible.
func TestHandledInDomain(t *testing.T) {
	origErr := domains.New("hello")
	t.Logf("origErr: %# v", pretty.Formatter(origErr))
	origDomain := domains.GetDomain(origErr)

	otherDomain := domains.NamedDomain("woo")
	err := domains.HandledInDomain(origErr, otherDomain)
	t.Logf("err: %# v", pretty.Formatter(err))

	tt := testutils.T{T: t}

	// The original domain becomes invisible.
	tt.Check(domains.NotInDomain(err, origDomain))

	// The cause becomes invisible.
	tt.Check(!markers.Is(err, origErr))

	// However, the error message is preserved fully.
	tt.CheckEqual(err.Error(), "hello")
}

// This test demonstrates that Handled() overrides an error's original
// domain with the current package's local domain.
func TestHandled(t *testing.T) {
	wooDomain := domains.NamedDomain("woo")
	origErr := domains.WithDomain(errors.New("hello"), wooDomain)

	tt := testutils.T{T: t}

	// Sanity check.
	tt.Assert(!domains.NotInDomain(origErr, wooDomain))

	// Handled overrides the domain with the current (package) domain.
	otherErr := domains.Handled(origErr)

	// The original domain disappears.
	tt.Check(domains.NotInDomain(otherErr, wooDomain))

	// The local domain appears.
	tt.Check(!domains.NotInDomain(otherErr, domains.PackageDomain()))
}

// This test demonstrates that same error annotated with different
// domains appears to be different errors for the purpose of Is()
// comparisons.
func TestDomainsBreakErrorEquivalence(t *testing.T) {
	baseErr := errors.New("hello")

	err1 := domains.WithDomain(baseErr, domains.NamedDomain("woo"))
	err2 := domains.WithDomain(baseErr, domains.NamedDomain("waa"))

	tt := testutils.T{T: t}

	tt.Check(!markers.Is(err1, err2))
	tt.Check(!markers.Is(err2, err1))
}
