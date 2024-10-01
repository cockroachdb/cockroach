// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Icon } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React, { HTMLAttributes, useState } from "react";

import styles from "./alert.module.scss";

const cx = classNames.bind(styles);

export enum AlertType {
  PRIMARY = "primary",
  SUCCESS = "success",
  WARNING = "warning",
  DANGER = "danger",
}

function getIcon(type: AlertType, className: string) {
  switch (type) {
    case AlertType.PRIMARY:
      return (
        <Icon
          fill="info"
          iconName="InfoCircleFilled"
          size="small"
          className={className}
        />
      );
    case AlertType.SUCCESS:
      return (
        <Icon
          fill="success"
          iconName="CheckCircleFilled"
          size="small"
          className={className}
        />
      );
    case AlertType.WARNING:
      return (
        <Icon
          fill="warning"
          iconName="Caution"
          size="small"
          className={className}
        />
      );
    case AlertType.DANGER:
      return (
        <Icon
          fill="danger"
          iconName="ErrorCircleFilled"
          size="small"
          className={className}
        />
      );
  }
}

type DocsLink = {
  text: string | "Learn more.";
  href: string;
};

type OwnProps = {
  className?: string;
  type: AlertType;
  title?: React.ReactNode;
  message?: React.ReactNode;
  docsLink?: DocsLink;
  additionalDocText?: React.ReactNode;
  expandableText?: string | React.ReactNode;
};

type NativeProps = Omit<HTMLAttributes<HTMLDivElement>, keyof OwnProps>;

export type AlertProps = OwnProps & NativeProps;

export function Alert({
  className,
  type,
  title,
  additionalDocText,
  message,
  expandableText,
  ...nativeProps
}: AlertProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div
      {...nativeProps}
      className={cx("alert", type.toLowerCase(), className)}
      role="alert"
    >
      <div className={cx("icon-container")}>{getIcon(type, cx("icon"))}</div>
      <div className={cx("content")}>
        {title && (
          <div className={cx("header")}>
            <div className={cx("title")}>{title}</div>
            {expandableText && (
              <Icon
                iconName={isExpanded ? "CaretDown" : "CaretRight"}
                size="small"
                onClick={() => setIsExpanded(prevState => !prevState)}
                className={cx("expandIcon")}
                aria-label="expand-text-icon"
              />
            )}
          </div>
        )}
        {message && <div className={cx("message")}>{message}</div>}
        {expandableText && isExpanded && (
          <div id="additional-text" className={cx("additionalText")}>
            {expandableText}
          </div>
        )}
        {additionalDocText && (
          <div className={cx("additionalText")}>{additionalDocText}</div>
        )}
      </div>
    </div>
  );
}
