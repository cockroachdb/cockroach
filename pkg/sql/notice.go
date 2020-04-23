// Copyright 2020 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// NoticesEnabled is the cluster setting that allows users
// to enable notices.
var NoticesEnabled = settings.RegisterPublicBoolSetting(
	"sql.notices.enabled",
	"enable notices in the server/client protocol being sent",
	true,
)

// noticeSender is a subset of RestrictedCommandResult which allows
// sending notices.
type noticeSender interface {
	AppendNotice(error)
}

// SendClientNotice implements the tree.ClientNoticeSender interface.
func (p *planner) SendClientNotice(ctx context.Context, err error) {
	if log.V(2) {
		log.Infof(ctx, "out-of-band notice: %+v", err)
	}
	noticeSeverity, ok := pgnotice.ParseDisplaySeverity(pgerror.GetSeverity(err))
	if !ok {
		noticeSeverity = pgnotice.DisplaySeverityNotice
	}
	if p.noticeSender == nil ||
		noticeSeverity > p.SessionData().NoticeDisplaySeverity ||
		!NoticesEnabled.Get(&p.execCfg.Settings.SV) {
		// Notice cannot flow to the client - because of one of these conditions:
		// * there is no client
		// * the session's NoticeDisplaySeverity is higher than the severity of the notice.
		// * the notice protocol was disabled
		return
	}
	p.noticeSender.AppendNotice(err)
}
