// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// inspectExecOverride returns the InternalExecutorOverride to use for
// INSPECT queries that scan tableDesc. Queries run as the table owner
// with implicit SELECT on the scanned descriptor and with row-level
// security bypassed for that descriptor so that policies on the
// inspected table cannot hide rows from a corruption check.
func inspectExecOverride(
	tableDesc catalog.TableDescriptor, qos *sessiondatapb.QoSLevel,
) sessiondata.InternalExecutorOverride {
	selectBit := privilege.List{privilege.SELECT}.ToBitField()
	return sessiondata.InternalExecutorOverride{
		User:             tableDesc.GetPrivileges().Owner(),
		QualityOfService: qos,
		DescriptorOverrides: map[uint32]sessiondata.DescriptorOverride{
			uint32(tableDesc.GetID()): {Privileges: selectBit, BypassRLS: true},
		},
	}
}
