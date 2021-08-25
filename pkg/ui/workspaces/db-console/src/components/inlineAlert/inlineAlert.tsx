// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";
import classNames from "classnames/bind";

import styles from "./inlineAlert.module.styl";
import ErrorIcon from "assets/error-circle.svg";
import InfoIcon from "assets/info-filled-circle.svg";

export type InlineAlertIntent = "info" | "error";

const cn = classNames.bind(styles);

export interface InlineAlertProps {
  title: React.ReactNode;
  message?: React.ReactNode;
  intent?: InlineAlertIntent;
  className?: string;
}

export const InlineAlert: React.FC<InlineAlertProps> = ({
  title,
  message,
  intent = "info",
  className,
}) => {
  const Icon = useMemo(() => {
    switch (intent) {
      case "error":
        return ErrorIcon;
      case "info":
      default:
        return InfoIcon;
    }
  }, [intent]);

  return (
    <div className={cn("root", `intent-${intent}`, className)}>
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
