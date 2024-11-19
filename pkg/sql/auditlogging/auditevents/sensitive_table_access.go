// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auditevents

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

var errTxnIsNotOpen = errors.New("txn is already committed or rolled back")

// SensitiveTableAccessEvent identifies accesses on tables that have been configured
// for auditing. See EXPERIMENTAL_AUDIT.
type SensitiveTableAccessEvent struct {
	TableDesc catalog.TableDescriptor
	Writing   bool
}

// BuildAuditEvent implements the auditlogging.AuditEventBuilder interface
func (s *SensitiveTableAccessEvent) BuildAuditEvent(
	ctx context.Context,
	assembler auditlogging.Auditor,
	details eventpb.CommonSQLEventDetails,
	exec eventpb.CommonSQLExecDetails,
) logpb.EventPayload {
	var err error
	var tn *tree.TableName
	tableName := ""
	mode := "r"
	if s.Writing {
		mode = "rw"
	}
	if assembler.Txn() != nil && assembler.Txn().IsOpen() {
		// Only open txn accepts further commands.
		tn, err = assembler.GetQualifiedTableNameByID(ctx, int64(s.TableDesc.GetID()), tree.ResolveRequireTableDesc)
	} else {
		err = errTxnIsNotOpen
	}
	if err != nil {
		log.Warningf(ctx, "name for audited table ID %d not found: %v", s.TableDesc.GetID(), err)
	} else {
		tableName = tn.FQString()
	}
	details.DescriptorID = uint32(s.TableDesc.GetID())
	return &eventpb.SensitiveTableAccess{
		CommonSQLEventDetails: details,
		CommonSQLExecDetails:  exec,
		TableName:             tableName,
		AccessMode:            mode,
	}
}
