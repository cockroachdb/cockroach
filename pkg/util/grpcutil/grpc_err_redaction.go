// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package grpcutil

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
	"github.com/cockroachdb/redact"
	"github.com/gogo/status"
)

func grpcSpecialCasePrintFn(err error, p errors.Printer, isLeaf bool) (handled bool, next error) {
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
