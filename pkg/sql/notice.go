// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
var NoticesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.notices.enabled",
	"enable notices in the server/client protocol being sent",
	true,
	settings.WithPublic)

// noticeSender is a subset of RestrictedCommandResult which allows
// sending notices.
type noticeSender interface {
	// BufferNotice buffers the given notice to be sent to the client before
	// the connection is closed. The notice will be in the command result buffer,
	// meaning that it will not be sent if the result buffer is discarded.
	BufferNotice(pgnotice.Notice)
	// SendNotice sends the given notice to the client. The notice will be in
	// the client communication buffer until it is flushed. Flushing can be forced
	// to occur immediately by setting immediateFlush to true.
	SendNotice(ctx context.Context, notice pgnotice.Notice, immediateFlush bool) error
}

// BufferClientNotice implements the eval.ClientNoticeSender interface.
func (p *planner) BufferClientNotice(ctx context.Context, notice pgnotice.Notice) {
	if log.V(2) {
		log.Infof(ctx, "buffered notice: %+v", notice)
	}
	if !p.checkNoticeSeverity(notice) {
		return
	}
	p.noticeSender.BufferNotice(notice)
}

// SendClientNotice implements the eval.ClientNoticeSender interface.
func (p *planner) SendClientNotice(
	ctx context.Context, notice pgnotice.Notice, immediateFlush bool,
) error {
	if log.V(2) {
		log.Infof(ctx, "sending notice: %+v", notice)
	}
	if !p.checkNoticeSeverity(notice) {
		return nil
	}
	return p.noticeSender.SendNotice(ctx, notice, immediateFlush)
}

func (p *planner) checkNoticeSeverity(notice pgnotice.Notice) bool {
	noticeSeverity, ok := pgnotice.ParseDisplaySeverity(pgerror.GetSeverity(notice))
	if !ok {
		noticeSeverity = pgnotice.DisplaySeverityNotice
	}
	// The notice can only flow to the client if the following are true:
	// * there is a client
	// * notice severity >= the session's NoticeDisplaySeverity
	// * the notice protocol is enabled
	// An exception to the second rule is DisplaySeverityInfo, which is always
	// sent to the client if notices are enabled.
	clientExists := p.noticeSender != nil
	display := noticeSeverity <= pgnotice.DisplaySeverity(p.SessionData().NoticeDisplaySeverity) ||
		noticeSeverity == pgnotice.DisplaySeverityInfo
	noticeEnabled := NoticesEnabled.Get(&p.execCfg.Settings.SV)
	return clientExists && display && noticeEnabled
}
