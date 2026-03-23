// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CockroachMarkLightFull } from "@cockroachlabs/icons";
import classNames from "classnames";
import React from "react";

import { AlertInfo, AlertLevel } from "src/redux/alerts";
import {
  warningIcon,
  criticalIcon,
  informationIcon,
} from "src/views/shared/components/icons";

import "./alertbox.scss";

function alertIcon(level: AlertLevel) {
  switch (level) {
    case AlertLevel.CRITICAL:
      return criticalIcon;
    case AlertLevel.WARNING:
      return warningIcon;
    case AlertLevel.INFORMATION:
      return informationIcon;
    default:
      return <CockroachMarkLightFull />;
  }
}

export interface AlertBoxProps extends AlertInfo {
  dismiss(): void;
}

export function AlertBox({
  title,
  text,
  link,
  level,
  dismiss,
}: AlertBoxProps): React.ReactElement {
  // build up content element, which has a wrapping anchor element that is
  // conditionally present.
  let content = (
    <div>
      <div className="alert-box__title">{title}</div>
      <div className="alert-box__text">{text}</div>
    </div>
  );

  const learnMore = link && (
    <a className="" href={link}>
      Learn More
    </a>
  );
  content = (
    <>
      <div className="alert-box__content">
        {content}
        {learnMore}
      </div>
    </>
  );

  return (
    <div
      className={classNames(
        "alert-box",
        `alert-box--${AlertLevel[level].toLowerCase()}`,
      )}
    >
      <div className="alert-box__icon">{alertIcon(level)}</div>
      {content}
      <div className="alert-box__dismiss">
        <a className="alert-box__link" onClick={dismiss}>
          ✕
        </a>
      </div>
    </div>
  );
}
