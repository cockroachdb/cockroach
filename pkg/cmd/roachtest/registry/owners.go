// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/gogo/protobuf/proto"
)

// Owner is a valid entry for the Owners field of a roachtest. They should be
// teams, not individuals.
type Owner string

// The allowable values of Owner.
const (
	OwnerSQLExperience    Owner = `sql-experience`
	OwnerDisasterRecovery Owner = `disaster-recovery`
	OwnerCDC              Owner = `cdc`
	OwnerKV               Owner = `kv`
	OwnerAdmissionControl Owner = `admission-control`
	OwnerMultiRegion      Owner = `multiregion`
	OwnerObsInf           Owner = `obs-inf-prs`
	OwnerServer           Owner = `server` // not currently staffed
	OwnerSQLQueries       Owner = `sql-queries`
	OwnerSQLSchema        Owner = `sql-schema`
	OwnerStorage          Owner = `storage`
	OwnerTestEng          Owner = `test-eng`
	OwnerDevInf           Owner = `dev-inf`
	OwnerMultiTenant      Owner = `multi-tenant`
)

func init() {
	pKey := errors.GetTypeKey(&ownedError{})
	errors.RegisterWrapperEncoder(pKey, encodeOwnedError)
	errors.RegisterWrapperDecoder(pKey, decodeOwnedError)
}

func encodeOwnedError(
	_ context.Context, err error,
) (msgPrefix string, safe []string, details proto.Message) {
	var e *ownedError
	errors.As(err, &e)
	details = &errorspb.StringsPayload{
		Details: []string{string(e.owner)},
	}
	return "", nil, details
}

func decodeOwnedError(
	ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message,
) error {
	m, ok := payload.(*errorspb.StringsPayload)
	if !ok || len(m.Details) != 1 {
		return nil
	}
	owner := Owner(m.Details[0])
	return &ownedError{
		owner:   owner,
		wrapped: cause,
	}
}

type ownedError struct {
	owner   Owner
	wrapped error
}

// Unwrap implements errors.Wrapper.
func (e *ownedError) Unwrap() error {
	return e.wrapped
}

// Format implements fmt.Formatter.
func (e *ownedError) Format(f fmt.State, verb rune) {
	errors.FormatError(e, f, verb)
}

// FormatError implements errors.Formatter.
func (e *ownedError) FormatError(p errbase.Printer) (next error) {
	p.Printf("owned by %s: %s", e.owner, e.wrapped)
	return e.wrapped
}

func (e *ownedError) Error() string {
	return fmt.Sprint(e)
}

// WrapWithOwner wraps the error with a hint at who should own the
// problem. The hint can later be retrieved via OwnerFromErr.
func WrapWithOwner(err error, owner Owner) error {
	return &ownedError{
		owner:   owner,
		wrapped: err,
	}
}

// OwnerFromErr returns the Owner associated previously via WrapWithOwner, to the error,
// if any.
func OwnerFromErr(err error) (Owner, bool) {
	var oe *ownedError
	if errors.As(err, &oe) {
		return oe.owner, true
	}
	return "", false
}
