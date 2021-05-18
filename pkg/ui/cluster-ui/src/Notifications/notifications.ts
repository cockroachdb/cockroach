// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This is a placeholder for real implementation (likely in Redux?) of notifications

import { notificationTypes, NotificationProps } from "../Notifications";
import { NotificationMessageProps } from "../NotificationMessage";

export const generateNotificationProps = (
  notifications: Array<NotificationProps>,
): Array<NotificationMessageProps> =>
  notifications.map(n => {
    const type = notificationTypes.find(nt => nt.key === n.type);
    return { ...n, ...type };
  });
