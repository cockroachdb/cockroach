// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgnotification

import (
	"fmt"

	"github.com/jackc/pgconn"
)

type Notification struct {
	Channel string
	Payload string
	PID     int32
}


// Stringify returns a human-readable string for the input pgconn.Notification
// that matches psql's output for async notifications.
func Stringify(notification *pgconn.Notification) string {
	var payloadStr string
	if len(notification.Payload) > 0 {
		payloadStr = fmt.Sprintf(" with payload %q", notification.Payload)
	}
	return fmt.Sprintf(
		"Asynchronous notification %q%s received from server with node ID %d.",
		notification.Channel, payloadStr, notification.PID,
	)
}
