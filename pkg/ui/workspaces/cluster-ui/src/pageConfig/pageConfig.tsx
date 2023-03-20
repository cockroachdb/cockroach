// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import classnames from "classnames/bind";
import styles from "./pageConfig.module.scss";
import { CockroachCloudContext } from "../contexts";

export interface PageConfigProps {
  layout?: "list" | "spread";
  children?: React.ReactNode;
  whiteBkg?: boolean;
  className?: string;
}

const cx = classnames.bind(styles);

export function PageConfig(props: PageConfigProps): React.ReactElement {
  const whiteBkg = useContext(CockroachCloudContext) || props.whiteBkg;

  const classes = cx({
    "page-config__list": props.layout !== "spread",
    "page-config__spread": props.layout === "spread",
  });

  return (
    <div
      className={`${cx("page-config", {
        "page-config__white-background": whiteBkg,
      })} ${props.className}`}
    >
      <ul className={classes}>{props.children}</ul>
    </div>
  );
}

export interface PageConfigItemProps {
  children?: React.ReactNode;
  className?: string;
}

export function PageConfigItem(props: PageConfigItemProps): React.ReactElement {
  return (
    <li className={`${cx("page-config__item")} ${props.className}`}>
      {props.children}
    </li>
  );
}
