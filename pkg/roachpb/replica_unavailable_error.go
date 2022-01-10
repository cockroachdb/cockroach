// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	context "context"
	"fmt"

	"github.com/cockroachdb/errors"
	proto "github.com/gogo/protobuf/proto"
)

// var _ errors.SafeFormatter = (*ReplicaUnavailableError)(nil)
var _ fmt.Formatter = (*ReplicaUnavailableError)(nil)

// SafeFormatError implements errors.SafeFormatter.
// TODO(tbg): giving up, this always gives me infinite recursion. It doesn't
// seem as though redact.Sprint checks for SafeFormatError first, instead
// it calls Error(), so I can't implement Error() via `redact.Sprint`. Ask Raphael.
// func (e *ReplicaUnavailableError) SafeFormatError(p errors.Printer) error {
// 	p.Printf("replica %s unable to serve request to %s", e.Replica, e.Desc)
// 	return nil
// }

// Format implements fmt.Formatter.
func (e *ReplicaUnavailableError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

func (e *ReplicaUnavailableError) Error() string {
	return fmt.Sprintf("replica %s unable to serve request to %s", e.Replica, e.Desc)
	//return redact.Sprint(e).StripMarkers()
}

func (e *ReplicaUnavailableError) String() string {
	return e.Error()
}

// PGCodeRangeUnavailable is a marker interface that tells `pgcode.ComputeDefaultCode`
// to use a PG error code of `pgcode.RangeUnavailable`.
//
// TODO(tbg): this is a workaround due to the import cycle bitarray -> pgerror
// -> roachpb -> bitarray that is not trivial to break.
func (e *ReplicaUnavailableError) PGCodeRangeUnavailable() {}

// NewReplicaUnavailableError initializes a new *ReplicaUnavailableError. It is
// provided with the range descriptor known to the replica, and the relevant
// replica descriptor within.
func NewReplicaUnavailableError(
	desc *RangeDescriptor, replDesc ReplicaDescriptor,
) *ReplicaUnavailableError {
	return &ReplicaUnavailableError{
		Desc:    *desc,
		Replica: replDesc,
	}
}

func init() {
	typeKey := errors.GetTypeKey((*ReplicaUnavailableError)(nil))
	errors.RegisterLeafEncoder(typeKey, encodeReplicaUnavailableError)
	errors.RegisterLeafDecoder(typeKey, decodeReplicaUnavailableError)
}

func encodeReplicaUnavailableError(
	_ context.Context, ierr error,
) (msgPrefix string, safeDetails []string, payload proto.Message) {
	err := *ierr.(*ReplicaUnavailableError)
	// TODO(tbg): should I be doing anything with the first two args?
	return "", []string{}, &err
}

func decodeReplicaUnavailableError(
	// TODO(tbg): should I be doing anything with msgPrefix and safeDetails?
	ctx context.Context,
	msgPrefix string,
	safeDetails []string,
	payload proto.Message,
) error {
	err := payload.(*ReplicaUnavailableError)
	return err
}
