// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { GetTraceResponse } from "src/api";
import Long from "long";
import { Loading } from "src/loading";
import React, { useEffect } from "react";
import classNames from "classnames/bind";
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
