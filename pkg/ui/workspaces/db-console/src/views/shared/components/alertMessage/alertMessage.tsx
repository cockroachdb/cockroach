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
import React from "react";
import { Link } from "react-router-dom";

import { AlertInfo, AlertLevel } from "src/redux/alerts";
import "./alertMessage.styl";

interface AlertMessageProps extends AlertInfo {
  autoClose: boolean;
  autoCloseTimeout: number;
  closable: boolean;
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

export class AlertMessage extends React.Component<AlertMessageProps> {
  static defaultProps = {
    closable: true,
    autoCloseTimeout: 6000,
  };

  timeoutHandler: number;

  componentDidMount() {
    const { autoClose, dismiss, autoCloseTimeout } = this.props;
    if (autoClose) {
      this.timeoutHandler = window.setTimeout(dismiss, autoCloseTimeout);
    }
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutHandler);
  }

  render() {
    const { level, dismiss, link, title, text, closable } = this.props;

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
}
