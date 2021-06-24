// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import _ from "lodash";

import CaretRight from "./caret-right.svg";
import Stack from "./stack.svg";
import Table from "./table.svg";

import styles from "./icon.module.scss";
const cx = classNames.bind(styles);

export type IconSize = "lg" | "md" | "s" | "xs" | "xxs";

export type IconType = "caret-right" | "stack" | "table";

function getIcon(iconType: IconType, className?: string): React.ReactNode {
  switch (iconType) {
    case "caret-right":
      return <img src={CaretRight} className={className} />;
    case "stack":
      return <img src={Stack} className={className} />;
    case "table":
      return <img src={Table} className={className} />;
    default:
      throw new Error(`Unknown icon ${iconType}`);
  }
}

export function Icon(props: {
  type: IconType;
  size?: IconSize;
  className?: string;
  children?: React.ReactChild;
}) {
  const containerClasses = cx("crl-icon__container", props.className);

  const iconClassName = cx("crl-icon", `crl-icon--${props.size || "md"}`);

  return (
    <div className={containerClasses}>
      {getIcon(props.type, iconClassName)}
      {props.children}
    </div>
  );
}
