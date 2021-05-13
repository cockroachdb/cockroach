import React, { FunctionComponent } from "react";
import classnames from "classnames/bind";
import { Badge, BadgeIntent, FuzzyTime } from "@cockroachlabs/ui-components";

import {
  NotificationTypeProp,
  NotificationProps,
  NotificationSeverity,
} from "../Notifications";

import styles from "./notificationMessage.module.scss";

export type NotificationMessageProps = NotificationTypeProp & NotificationProps;

const cx = classnames.bind(styles);

const truncate = (string: string, length: number): string => {
  if (string.length <= length) return string;

  return `${string.slice(0, length)}…`;
};

const severityIntent = (s: NotificationSeverity): BadgeIntent => {
  const intentMap = {
    low: "neutral",
    info: "neutral",
    moderate: "info",
    critical: "info",
  };
  return intentMap[s] as BadgeIntent;
};

export const NotificationMessage: FunctionComponent<NotificationMessageProps> = ({
  id,
  description,
  read,
  severity,
  timestamp,
  title,
}) => {
  const time = new Date(timestamp);
  return (
    <section key={id} className={cx("notification-message", { unread: !read })}>
      <header className={cx("title")}>{title}</header>
      <Badge className={cx("severity")} intent={severityIntent(severity)}>
        {severity}
      </Badge>
      {description && (
        <div className={cx("description")}>{truncate(description, 120)}</div>
      )}
      <div className={cx("timestamp")}>
        <FuzzyTime timestamp={time} />
      </div>
    </section>
  );
};

export default NotificationMessage;
