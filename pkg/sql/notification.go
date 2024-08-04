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

// // Notifications is the cluster setting that allows users
// // to enable notifications.
// var Notifications = settings.RegisterBoolSetting(
// 	settings.ApplicationLevel,
// 	"sql.notifications.enabled",
// 	"enable notifications in the server/client protocol being sent",
// 	true,
// 	settings.WithPublic)

// type notificationSender interface {
// 	SendNotification(pgnotification.Notification) error
// }

// // SendClientNotice implements the eval.ClientNotificationSender interface.
// func (p *planner) SendClientNotification(ctx context.Context, notification pgnotification.Notification) {
// 	if log.V(2) {
// 		log.Infof(ctx, "buffered notification: %+v", notification)
// 	}
// 	if !Notifications.Get(&p.ExecCfg().Settings.SV) {
// 		return
// 	}
// 	if err := p.notificationSender.SendNotification(notification); err != nil {
// 		// This is just an artifact of the dummy impl, probably.
// 		log.Errorf(ctx, "buffering notification: %v", err)
// 	}
// }
