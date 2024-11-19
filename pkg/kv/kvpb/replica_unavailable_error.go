// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	context "context"
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// NewReplicaUnavailableError initializes a new *ReplicaUnavailableError. It is
// provided with the range descriptor known to the replica, and the relevant
// replica descriptor within.
func NewReplicaUnavailableError(
	cause error, desc *roachpb.RangeDescriptor, replDesc roachpb.ReplicaDescriptor,
) error {
	return &ReplicaUnavailableError{
		Desc:    *desc,
		Replica: replDesc,
		Cause:   errors.EncodeError(context.Background(), cause),
	}
}

var _ errors.SafeFormatter = (*ReplicaUnavailableError)(nil)
var _ fmt.Formatter = (*ReplicaUnavailableError)(nil)
var _ errors.Wrapper = (*ReplicaUnavailableError)(nil)

// SafeFormatError implements errors.SafeFormatter.
func (e *ReplicaUnavailableError) SafeFormatError(p errors.Printer) error {
	p.Printf("replica unavailable: %s unable to serve request to %s: %s", e.Replica, e.Desc, e.Unwrap())
	return nil
}

// Format implements fmt.Formatter.
func (e *ReplicaUnavailableError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// Error implements error.
func (e *ReplicaUnavailableError) Error() string {
	return fmt.Sprint(e)
}

// Unwrap implements errors.Wrapper.
func (e *ReplicaUnavailableError) Unwrap() error {
	return errors.DecodeError(context.Background(), e.Cause)
}

func init() {
	// Register the migration of the error that used to be in the roachpb
	// package and is now in the kv/kvpb package.
	roachpbPath := reflect.TypeOf(roachpb.Key("")).PkgPath()
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.ReplicaUnavailableError", &ReplicaUnavailableError{})
	// Note that it is important that these wrapper methods are registered
	// _after_ the type migration above.
	encode := func(ctx context.Context, err error) (msgPrefix string, safeDetails []string, payload proto.Message) {
		errors.As(err, &payload) // payload = err.(proto.Message)
		return "", nil, payload
	}
	decode := func(ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message) error {
		return payload.(*ReplicaUnavailableError)
	}
	typeName := errors.GetTypeKey((*ReplicaUnavailableError)(nil))
	errors.RegisterWrapperEncoder(typeName, encode)
	errors.RegisterWrapperDecoder(typeName, decode)
}
