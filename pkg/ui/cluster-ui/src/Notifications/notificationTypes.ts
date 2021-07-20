// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

export type NotificationType =
  | "backup-blocked"
  | "command-commit"
  | "expired"
  | "full-table"
  | "network-partition";
export type NotificationSeverity = "low" | "info" | "moderate" | "critical";
export type NotificationTypeProp = {
  key: string;
  title: string;
  description: string;
  severity: NotificationSeverity;
};
export type NotificationProps = {
  id: number;
  read: boolean;
  timestamp: string;
  type: NotificationType;
};

export const notificationTypes: Array<NotificationTypeProp> = [
  {
    key: "backup-blocked",
    title: "Backup blocked on long-running Transaction",
    description:
      "There is a long running transaction that has prevented a backup on a table for more than 1 hour.",
    severity: "low",
  },
  {
    key: "command-commit",
    title: "Command Commit Latency",
    description:
      "Command Commit Latency is > 100ms on at least one node in this cluster. This can result in poor query performance.",
    severity: "low",
  },
  {
    key: "expired",
    title: "Expired License Key",
    description:
      "Your enterprise license key has expired. Enterprise features are disabled until a new license key is set.",
    severity: "moderate",
  },
  {
    key: "full-table",
    title: "Full Table Scan",
    description:
      "There are queries resulting in full table scans in this cluster. Full table scans may result in poor query performance.",
    severity: "low",
  },
  {
    key: "network-partition",
    title: "Network Partition",
    description: "There may be a network partition in this cluster.",
    severity: "critical",
  },
];

export default notificationTypes;
