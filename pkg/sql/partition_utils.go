// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/regionutils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// GenerateSubzoneSpans see regionutils.GenerateSubzoneSpans for details.
// This wrapper enforces licensing restrictions which are enforced else,
// where in the declarative schema changer.
// TODO(benesch): remove the hasNewSubzones parameter when a statement to clear
// all subzones at once is introduced.
func GenerateSubzoneSpans(
	st *cluster.Settings,
	logicalClusterID uuid.UUID,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	subzones []zonepb.Subzone,
	hasNewSubzones bool,
) ([]zonepb.SubzoneSpan, error) {
	if hasNewSubzones {
		org := ClusterOrganization.Get(&st.SV)
		if err := base.CheckEnterpriseEnabled(st, logicalClusterID, org,
			"replication zones on indexes or partitions"); err != nil {
			return nil, err
		}
	}
	return regionutils.GenerateSubzoneSpans(codec, tableDesc, subzones)
}
