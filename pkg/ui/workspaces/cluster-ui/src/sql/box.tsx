// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Highlight } from "./highlight";
import classNames from "classnames/bind";

import styles from "./sqlhighlight.module.scss";
import { api as clusterUiApi } from "../index";
import { FormatQuery } from "src/util";

export enum SqlBoxSize {
  small = "small",
  large = "large",
  custom = "custom",
}

export interface SqlBoxProps {
  value: string;
  zone?: clusterUiApi.DatabaseDetailsResponse;
  className?: string;
  size?: SqlBoxSize;
  format?: boolean;
}

const cx = classNames.bind(styles);

export class SqlBox extends React.Component<SqlBoxProps> {
  preNode: React.RefObject<HTMLPreElement> = React.createRef();
  render(): React.ReactElement {
    const value = this.props.format
      ? FormatQuery(this.props.value)
      : this.props.value;
    const sizeClass = this.props.size ? this.props.size : "";
    return (
      <div className={cx("box-highlight", this.props.className, sizeClass)}>
        <Highlight {...this.props} value={value} />
      </div>
    );
  }
}
