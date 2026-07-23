// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import Long from "long";
import React, { useEffect } from "react";

import { GetTraceResponse } from "src/api";
import { Loading } from "src/loading";

import styles from "../snapshot.module.scss";
const cx = classNames.bind(styles);

export const RawTraceComponent: React.FC<{
  nodeID: string;
  snapshotID: number;
  traceID: Long;
  rawTrace: GetTraceResponse;
  rawTraceLoading: boolean;
  rawTraceError?: Error;
  refreshRawTrace: (req: {
    nodeID: string;
    snapshotID: number;
    traceID: Long;
  }) => void;
}> = props => {
  const {
    nodeID,
    snapshotID,
    traceID,
    rawTrace,
    rawTraceLoading,
    rawTraceError,
    refreshRawTrace,
  } = props;

  useEffect(() => {
    if (!(nodeID && snapshotID && traceID)) {
      return;
    }
    refreshRawTrace({
      nodeID,
      snapshotID,
      traceID,
    });
  }, [nodeID, snapshotID, traceID, refreshRawTrace]);

  return (
    <Loading
      loading={rawTraceLoading}
      page={"raw trace"}
      error={rawTraceError}
      render={() => {
        return (
          <>
            <section
              data-testid="raw-trace-component"
              className={cx("span-section")}
            >
              <pre>{rawTrace?.serialized_recording}</pre>
            </section>
            <div className={cx("bottom-padding")} />
          </>
        );
      }}
    />
  );
};
