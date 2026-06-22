// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var jobOwnerFallbackEvery = log.Every(time.Minute)

// jobOwnerOrNode returns u, falling back to the node user when u is unset.
// An empty SQLUsernameProto reaches the receiver only when the gateway is
// on a release that predates the LDR-spec UsernameProto field — i.e.
// during a rolling upgrade from an older version. The fallback restores
// the prior behavior on that flow; the new identity activates as soon
// as every gateway has been upgraded.
//
// TODO(mw5h): drop this fallback once direct upgrade from 26.2 is no
// longer a supported upgrade path.
func jobOwnerOrNode(ctx context.Context, u username.SQLUsernameProto) username.SQLUsernameProto {
	if !u.Decode().Undefined() {
		return u
	}
	if jobOwnerFallbackEvery.ShouldLog() {
		log.Dev.Warningf(ctx,
			"LDR spec missing job owner; falling back to NodeUser "+
				"(gateway on a pre-fix release during rolling upgrade?)")
	}
	return username.NodeUserName().EncodeProto()
}
