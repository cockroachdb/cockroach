// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { FunctionComponent, ReactNode } from "react";

import { NotificationProps, generateNotificationProps } from "../Notifications";
import { NotificationMessage } from "./index";

type ReactProps = {
  children: ReactNode;
};

export default {
  title: "Notfication Message",
  component: NotificationMessage,
};

const testNotifications: Array<NotificationProps> = [
  {
    id: 1,
    type: "backup-blocked",
    timestamp: "30 JUL 2020 13:22",
    read: true,
  },
  {
    id: 2,
    type: "command-commit",
    timestamp: "30 JUL 2020 13:25",
    read: false,
  },
  {
    id: 3,
    type: "expired",
    timestamp: "30 JUL 2020 13:30",
    read: false,
  },
  {
    id: 4,
    type: "full-table",
    timestamp: "30 JUL 2020 13:35",
    read: true,
  },
  {
    id: 5,
    type: "expired",
    timestamp: "30 JUL 2020 14:35",
    read: true,
  },
  {
    id: 6,
    type: "network-partition",
    timestamp: "12 AUG 2020 10:00",
    read: false,
  },
];

const notificationMessages = generateNotificationProps(testNotifications);

const NotificationsDemo: FunctionComponent<ReactProps> = ({ children }) => (
  <section style={{ padding: "2rem" }}>{children}</section>
);
const NotificationsMessageTypes: FunctionComponent<ReactProps> = ({
  children,
}) => (
  <section style={{ display: "flex", flexWrap: "wrap" }}>{children}</section>
);
const NotificationFrame: FunctionComponent<ReactProps> = ({ children }) => (
  <section
    style={{
      width: "30rem",
      padding: "2rem",
      border: "1px dotted #222",
      margin: "0 2rem 2rem 0",
    }}
  >
    {children}
  </section>
);

export const Demo = () => (
  <NotificationsDemo>
    <h1>Notification Types</h1>
    <NotificationsMessageTypes>
      {notificationMessages.map(n => (
        <section key={n.id}>
          <code>{n.type}</code>
          <NotificationFrame>
            <NotificationMessage {...n} />
          </NotificationFrame>
        </section>
      ))}
    </NotificationsMessageTypes>
  </NotificationsDemo>
);
