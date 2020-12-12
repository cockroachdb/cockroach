// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func (r *Replica) exitAndPreventStartupMuLockedRaftMuLocked(
	ctx context.Context,
	err error,
	preventStartupMsgBody string,
) {
	auxDir := r.store.engine.GetAuxiliaryDir()
	_ = r.store.engine.MkdirAll(auxDir)
	path := base.PreventedStartupFile(auxDir)

	preventStartupMsg := fmt.Sprintf(`ATTENTION:

%s

A file preventing this node from restarting was placed at:
%s
`, preventStartupMsgBody, path)

	if err := fs.WriteFile(r.store.engine, path, []byte(preventStartupMsg)); err != nil {
		log.Warningf(ctx, "%v", err)
	}

	log.FatalfDepth(ctx, 1, "replica is corrupted: %s", err)
}
