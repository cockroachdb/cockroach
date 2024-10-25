// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import React from "react";

import { FormatQuery } from "src/util";

import { Highlight } from "./highlight";
import styles from "./sqlhighlight.module.scss";

type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export enum SqlBoxSize {
  SMALL = "small",
  LARGE = "large",
  CUSTOM = "custom",
}

type DatabaseZoneConfig = {
  zone_config: ZoneConfigType;
  zone_config_level: ZoneConfigLevelType;
};

export interface SqlBoxProps {
  value: string;
  // (xinhaoz): Came across this while deleting legacy db pages.
  // It doesn't seem like there are any usages of this prop today.
  // It may have been from a time where we showed the create statement
  // for a database.
  // Created DatabaseZoneConfig as a replacement until we decide
  // whether to bring back create db statement.
  zone?: DatabaseZoneConfig;
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
