// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is a placeholder for real implementation (likely in Redux?) of notifications

import { NotificationMessageProps } from "../NotificationMessage";
import { notificationTypes, NotificationProps } from "../Notifications";

export const generateNotificationProps = (
  notifications: Array<NotificationProps>,
): Array<NotificationMessageProps> =>
  notifications.map(n => {
    const type = notificationTypes.find(nt => nt.key === n.type);
    return { ...n, ...type };
  });
