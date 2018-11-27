// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import React from "react";
import classNames from "classnames";

import "./alertbox.styl";

import { AlertInfo, AlertLevel } from "src/redux/alerts";
import { warningIcon, notificationIcon, criticalIcon } from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";

function alertIcon (level: AlertLevel) {
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
    let content = <div>
      <div className="alert-box__title">{this.props.title}</div>
      <div className="alert-box__text">{this.props.text}</div>
    </div>;

    if (this.props.link) {
      content = <a className="alert-box__link" href={this.props.link}>
        { content }
      </a>;
    }

    content = <div className="alert-box__content">
      { content }
    </div>;

    return <div className={classNames("alert-box", `alert-box--${AlertLevel[this.props.level].toLowerCase()}`)}>
      <div className="alert-box__icon" dangerouslySetInnerHTML={alertIcon(this.props.level)} />
      { content }
      <div className="alert-box__dismiss">
        <a className="alert-box__link" onClick={this.props.dismiss}>✕</a>
      </div>
    </div>;
  }
}
