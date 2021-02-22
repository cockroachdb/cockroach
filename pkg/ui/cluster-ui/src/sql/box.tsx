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
import { Highlight } from "./highlight";
import classNames from "classnames/bind";

import styles from "./sqlhighlight.module.scss";
import * as protos from "@cockroachlabs/crdb-protobuf-client";

export interface SqlBoxProps {
  value: string;
  zone?: protos.cockroach.server.serverpb.DatabaseDetailsResponse;
  className?: string;
}

const cx = classNames.bind(styles);

export class SqlBox extends React.Component<SqlBoxProps> {
  preNode: React.RefObject<HTMLPreElement> = React.createRef();
  render() {
    return (
      <div className={cx("box-highlight", this.props.className)}>
        <Highlight {...this.props} />
      </div>
    );
  }
}
