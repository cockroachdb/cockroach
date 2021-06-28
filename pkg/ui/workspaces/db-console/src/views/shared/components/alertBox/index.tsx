// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames";

import "./alertbox.styl";

import { AlertInfo, AlertLevel } from "src/redux/alerts";
import {
  warningIcon,
  notificationIcon,
  criticalIcon,
} from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";

function alertIcon(level: AlertLevel) {
  switch (level) {
    case AlertLevel.CRITICAL:
      return trustIcon(criticalIcon);
    case AlertLevel.WARNING:
      return trustIcon(warningIcon);
    default:
      return trustIcon(notificationIcon);
  }
}

export interface AlertBoxProps extends AlertInfo {
  dismiss(): void;
}

export class AlertBox extends React.Component<AlertBoxProps, {}> {
  render() {
    // build up content element, which has a wrapping anchor element that is
    // conditionally present.
    let content = (
      <div>
        <div className="alert-box__title">{this.props.title}</div>
        <div className="alert-box__text">{this.props.text}</div>
      </div>
    );

    if (this.props.link) {
      content = (
        <a className="alert-box__link" href={this.props.link}>
          {content}
        </a>
      );
    }

    content = <div className="alert-box__content">{content}</div>;

    return (
      <div
        className={classNames(
          "alert-box",
          `alert-box--${AlertLevel[this.props.level].toLowerCase()}`,
        )}
      >
        <div
          className="alert-box__icon"
          dangerouslySetInnerHTML={alertIcon(this.props.level)}
        />
        {content}
        <div className="alert-box__dismiss">
          <a className="alert-box__link" onClick={this.props.dismiss}>
            ✕
          </a>
        </div>
      </div>
    );
  }
}
