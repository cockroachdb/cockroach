// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
