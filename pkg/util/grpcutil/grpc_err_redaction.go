// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package grpcutil

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
	"github.com/cockroachdb/redact"
	"github.com/gogo/status"
	grpcStatus "google.golang.org/grpc/status"
)

func grpcSpecialCasePrintFn(err error, p errors.Printer, isLeaf bool) (handled bool, next error) {
	// If GRPCStatus() returns nil, status.FromError will still
	// return true, but the retuned error will have no message and
	// a code of OK.
	// nolint:errcmp
	if se, ok := err.(interface{ GRPCStatus() *grpcStatus.Status }); ok {
		if se.GRPCStatus() == nil {
			return false, nil
		}
	}

	s, ok := status.FromError(err)
	if !ok {
		return false, nil
	}

	p.Printf("grpc: %s [code %[2]d/%[2]s]", s.Message(), redact.Safe(s.Code()))
	for _, d := range s.Details() {
		p.Printf(", %s", d)
	}
	return true, nil
}

func init() {
	errbase.RegisterSpecialCasePrinter(grpcSpecialCasePrintFn)
}
