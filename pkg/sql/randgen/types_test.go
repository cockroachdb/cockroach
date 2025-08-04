// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// TestSeedTypes verifies that at least one representative type is included into
// SeedTypes for all (with a few exceptions) type families.
func TestSeedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	noFamilyRepresentative := make(map[types.Family]struct{})
loop:
	for id := range types.Family_name {
		familyID := types.Family(id)
		switch familyID {
		case types.EnumFamily:
			// Enums need to created separately.
			continue loop
		case types.EncodedKeyFamily:
			// It's not a real type.
			continue loop
		case types.UnknownFamily, types.AnyFamily, types.TriggerFamily:
			// These are not included on purpose.
			continue loop
		case types.JsonpathFamily:
			// TODO(#22513): Don't include jsonpath in randomized tests yet.
			continue loop
		}
		noFamilyRepresentative[familyID] = struct{}{}
	}
	for _, typ := range SeedTypes {
		delete(noFamilyRepresentative, typ.Family())
	}
	if len(noFamilyRepresentative) > 0 {
		s := "no representative for "
		for f := range noFamilyRepresentative {
			s += fmt.Sprintf("%s (%d) ", types.Family_name[int32(f)], f)
		}
		t.Fatal(errors.Errorf("%s", s))
	}
}

func TestCanonical(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()
	for range 1000 {
		typ := RandType(rng)

		if !typ.Canonical().Equivalent(typ.WithoutTypeModifiers()) {
			t.Fail()
			t.Logf("fail: canonical type of %+v should be equivalent to the type without modifiers", typ)
		}

		datum := RandDatum(rng, typ, false)
		datumTyp := datum.ResolvedType()
		if !datumTyp.Equivalent(typ.Canonical()) {
			t.Fail()
			t.Logf("fail: canonical type of %+v is %+v and the datum's type is %+v", typ, typ.Canonical(), datumTyp)
		}

		if datumTyp.Oid() != typ.Canonical().Oid() {
			t.Fail()
			t.Logf("fail type %+v: canonical type oid %d does not match the datum's (%+v) oid %+v",
				typ, typ.Canonical().Oid(), datum, datumTyp.Oid())
		}
	}
}
