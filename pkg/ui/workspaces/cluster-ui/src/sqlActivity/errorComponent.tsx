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
import moment, { Moment } from "moment-timezone";

const cx = classNames.bind(styles);

interface SQLActivityErrorProps {
  statsType: string;
  error: Error;
}

export function mergeErrors(errs: Error | Error[]): Error {
  if (!errs) {
    return null;
  }

  if (!Array.isArray(errs)) {
    // Put single Error into a list to simplify logic in main Loading component.
    return errs;
  }

  const errors: Error[] = errs as Error[];

  if (!errors) {
    return null;
  }

  if (errors.length === 0) {
    return null;
  }

  if (errors.length === 1) {
    return errors[0];
  }

  const mergedError: Error = {
    name: "Multiple errors: ",
    message: "Multiple errors: ",
  };

  errors.forEach(
    (x, i, arr) => (
      (mergedError.name += ` ${i}: ${x.name};`),
      (mergedError.message += ` ${i}: ${x.message};`)
    ),
  );
  return mergedError;
}

const LoadingError: React.FC<SQLActivityErrorProps> = props => {
  const url = window.location.href;
  if (props.error && props.error.name === "GetDatabaseInfoError") {
    return (
      <div className={cx("row")}>
        <span>{props.error.message}</span>
        <br />
        <span>{`Debug information: ${moment
          .utc()
          .format("YYYY.MM.DD HH:mm:ss")} utc; URL: ${url}`}</span>
      </div>
    );
  }

  const error = props.error?.name?.toLowerCase().includes("timeout")
    ? "a timeout"
    : "an unexpected error";

  return (
    <div>
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
      <div className={cx("row-small")}>
        <br />
        <span>{`Debug information: ${moment
          .utc()
          .format("YYYY.MM.DD HH:mm:ss")} utc; Error message: ${
          props?.error?.message
        }; URL: ${url};`}</span>
      </div>
    </div>
  );
};

export default LoadingError;
