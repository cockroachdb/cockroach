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
import styles from "./sqlActivity.module.scss";

const cx = classNames.bind(styles);

interface SQLActivityErrorProps {
  statsType: string;
  timeout?: boolean;
}

const SQLActivityError: React.FC<SQLActivityErrorProps> = props => {
  const error = props.timeout ? "a timeout" : "an unexpected error";
  return (
    <div className={cx("row")}>
      <span>{`This page had ${error} while loading ${props.statsType}.`}</span>
      &nbsp;
      <a
        className={cx("action")}
        onClick={() => {
          window.location.reload();
        }}
      >
        Reload this page
      </a>
    </div>
  );
};

export default SQLActivityError;
