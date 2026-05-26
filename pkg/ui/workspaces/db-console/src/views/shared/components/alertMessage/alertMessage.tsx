// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  CheckCircleFilled,
  CloseCircleFilled,
  InfoCircleFilled,
  WarningFilled,
} from "@ant-design/icons";
import { Alert } from "antd";
import React, { useEffect, useRef } from "react";
import { Link } from "react-router-dom";

import { AlertInfo, AlertLevel } from "src/redux/alerts";
import "./alertMessage.scss";

interface AlertMessageProps extends AlertInfo {
  autoClose?: boolean;
  autoCloseTimeout?: number;
  closable?: boolean;
  dismiss(): void;
}

type AlertType = "success" | "info" | "warning" | "error";

const mapAlertLevelToType = (alertLevel: AlertLevel): AlertType => {
  switch (alertLevel) {
    case AlertLevel.SUCCESS:
      return "success";
    case AlertLevel.NOTIFICATION:
      return "info";
    case AlertLevel.WARNING:
      return "warning";
    case AlertLevel.CRITICAL:
      return "error";
    default:
      return "info";
  }
};

const getIcon = (alertLevel: AlertLevel): React.ReactNode => {
  switch (alertLevel) {
    case AlertLevel.SUCCESS:
      return <CheckCircleFilled className="alert-massage__icon" />;
    case AlertLevel.NOTIFICATION:
      return <InfoCircleFilled className="alert-massage__icon" />;
    case AlertLevel.WARNING:
      return <WarningFilled className="alert-massage__icon" />;
    case AlertLevel.CRITICAL:
      return <CloseCircleFilled className="alert-massage__icon" />;
    default:
      return <InfoCircleFilled className="alert-massage__icon" />;
  }
};

export function AlertMessage({
  autoClose = false,
  autoCloseTimeout = 6000,
  closable = true,
  dismiss,
  level,
  link,
  title,
  text,
}: AlertMessageProps): React.ReactElement {
  const timeoutRef = useRef<number>();

  useEffect(() => {
    if (autoClose) {
      timeoutRef.current = window.setTimeout(dismiss, autoCloseTimeout);
    }
    return () => {
      clearTimeout(timeoutRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  let description: React.ReactNode = text;

  if (link) {
    description = (
      <Link to={link} onClick={dismiss}>
        {text}
      </Link>
    );
  }

  const type = mapAlertLevelToType(level);
  return (
    <Alert
      className="alert-massage"
      message={title}
      description={description}
      showIcon
      icon={getIcon(level)}
      closable={closable}
      onClose={dismiss}
      type={type}
      banner
    />
  );
}
