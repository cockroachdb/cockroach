// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { Alert, Icon } from "antd";
import { Link } from "react-router";

import { AlertInfo, AlertLevel } from "src/redux/alerts";

import "./alertMessage.styl";

interface AlertMessageProps extends AlertInfo {
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

const getIconType = (alertLevel: AlertLevel): string => {
  switch (alertLevel) {
    case AlertLevel.SUCCESS:
      return "check-circle";
    case AlertLevel.NOTIFICATION:
      return "info-circle";
    case AlertLevel.WARNING:
      return "warning";
    case AlertLevel.CRITICAL:
      return "close-circle";
    default:
      return "info-circle";
  }
};

export class AlertMessage extends React.Component<AlertMessageProps> {
  static defaultProps: Partial<AlertMessageProps> = {
    link: undefined,
    text: undefined,
    wrapTextWithLink: undefined,
  };

  render() {
    const {
      level,
      dismiss,
      link,
      title,
      text,
      wrapTextWithLink,
    } = this.props;

    let description: React.ReactNode = text;

    if (link && wrapTextWithLink && text.includes(wrapTextWithLink)) {
      const [beforeLinkSubstring, afterLinkSubstring] = text.split(wrapTextWithLink, 2);

      description = (
        <React.Fragment>
          <span>{beforeLinkSubstring}</span>
          <Link to={link} onClick={dismiss}>{wrapTextWithLink}</Link>
          <span>{afterLinkSubstring}</span>
        </React.Fragment>);
    }

    const type = mapAlertLevelToType(level);
    const iconType = getIconType(level);
    return (
      <Alert
        className="alert-massage"
        message={title}
        description={description}
        showIcon
        icon={<Icon type={iconType} theme="filled" className="alert-massage__icon" />}
        closable
        onClose={dismiss}
        type={type} />
    );
  }
}
