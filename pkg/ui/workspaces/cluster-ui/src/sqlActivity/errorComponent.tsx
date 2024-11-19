// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import moment from "moment-timezone";
import React from "react";

import { isRequestError, RequestError } from "../util";

import styles from "./sqlActivity.module.scss";

const cx = classNames.bind(styles);

interface SQLActivityErrorProps {
  statsType: string;
  error: Error | RequestError;
  sourceTables?: string[];
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
    (x, i) => (
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

  const tablesInfo =
    props.sourceTables?.length === 1
      ? `Source Table: ${props.sourceTables[0]}`
      : props.sourceTables?.length > 1
        ? `Source Tables: ${props.sourceTables.join(", ")}`
        : "";

  const respCode = isRequestError(props?.error)
    ? props?.error?.status
    : undefined;

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
        <span>
          {`Debug information: ${moment
            .utc()
            .format("YYYY.MM.DD HH:mm:ss")} utc;`}
          <br />
          {respCode && (
            <>
              {`Response code: ${respCode};`}
              <br />
            </>
          )}
          {`Error message: ${props?.error?.message};`}
          <br />
          {`URL: ${url};`}
          <br />
          {tablesInfo}
        </span>
      </div>
    </div>
  );
};

export default LoadingError;
