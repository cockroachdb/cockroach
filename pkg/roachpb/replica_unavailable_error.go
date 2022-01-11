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
	"github.com/cockroachdb/redact"
	proto "github.com/gogo/protobuf/proto"
)

func init() {
	typeKey := errors.GetTypeKey((*ReplicaUnavailableError)(nil))
	errors.RegisterLeafEncoder(typeKey, encodeReplicaUnavailableError)
	errors.RegisterLeafDecoder(typeKey, decodeReplicaUnavailableError)
}

func encodeReplicaUnavailableError(
	_ context.Context, err error,
) (msgPrefix string, safeDetails []string, payload proto.Message) {
	// TODO(tbg): should I be doing anything with the first two args?
	var msg proto.Message
	_ = errors.As(err, &msg)
	return "", []string{}, msg
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

var _ errors.SafeFormatter = (*ReplicaUnavailableError)(nil)
var _ fmt.Formatter = (*ReplicaUnavailableError)(nil)

// TODO(tbg): I couldn't get this to work. I thought the "right way" to do
// things would be to implement errors.SafeFormatter, and that all the other
// impls (in particular Error) would fall out of that. In particular I wanted
// to avoid implementing `Error() string` and then basically duplicating that
// in whatever the right redaction method is. However, `Error()` becomes part
// of the error mark, so it is called during `fmt.Sprint(err)` and you get
// infinite recursion. I'm unsure how to best resolve that.

// SafeFormatError implements errors.SafeFormatter.
func (e *ReplicaUnavailableError) SafeFormatError(p errors.Printer) error {
	p.Printf("replica %s unable to serve request to %s", e.Replica, e.Desc)
	return nil
}

// Format implements fmt.Formatter.
func (e *ReplicaUnavailableError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// Error implements error.
func (e *ReplicaUnavailableError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *ReplicaUnavailableError) String() string {
	return e.Error()
}

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
