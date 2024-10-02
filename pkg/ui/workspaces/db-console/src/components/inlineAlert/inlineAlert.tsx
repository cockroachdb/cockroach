// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React, { useMemo } from "react";

import ErrorIcon from "assets/error-circle.svg";
import InfoIcon from "assets/info-filled-circle.svg";
import WarningIcon from "assets/warning.svg";

import styles from "./inlineAlert.module.styl";

export type InlineAlertIntent = "info" | "error" | "warning";

const cn = classNames.bind(styles);

export interface InlineAlertProps {
  title: React.ReactNode;
  message?: React.ReactNode;
  intent?: InlineAlertIntent;
  className?: string;
  fullWidth?: boolean;
}

export const InlineAlert: React.FC<InlineAlertProps> = ({
  title,
  message,
  intent = "info",
  className,
  fullWidth,
}) => {
  const Icon = useMemo(() => {
    switch (intent) {
      case "error":
        return ErrorIcon;
      case "warning":
        return WarningIcon;
      case "info":
      default:
        return InfoIcon;
    }
  }, [intent]);

  return (
    <div
      className={cn(
        "root",
        `intent-${intent}`,
        { "full-width": fullWidth },
        className,
      )}
    >
      <div className={cn("icon-container")}>
        <img src={Icon} className={cn("icon")} />
      </div>
      <div className={cn("main-container")}>
        <div className={cn("title")}>{title}</div>
        <div className={cn("message")}>{message}</div>
      </div>
    </div>
  );
};
