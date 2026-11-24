// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/revlog/restorerevlog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
)

func init() {
	rowexec.NewRevlogLocalMergeProcessor = restorerevlog.NewRevlogLocalMergeProcessor
	restorerevlog.NewKeyRewriter = func(
		codec keys.SQLCodec,
		tableRekeys []execinfrapb.TableRekey,
		tenantRekeys []execinfrapb.TenantRekey,
	) (restorerevlog.RewriteKeyFn, error) {
		kr, err := MakeKeyRewriterFromRekeys(
			codec, tableRekeys, tenantRekeys,
			false, /* restoreTenantFromStream */
		)
		if err != nil {
			return nil, err
		}
		return func(key []byte) ([]byte, bool, error) {
			return kr.RewriteKey(key, 0 /* walltimeForImportElision */)
		}, nil
	}
}
