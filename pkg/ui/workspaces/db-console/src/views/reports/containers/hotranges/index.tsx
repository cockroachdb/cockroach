// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useCallback, useEffect, useState } from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";
import moment from "moment";
import { Button } from "@cockroachlabs/ui-components";
import { cockroach } from "src/js/protos";
import { getHotRanges } from "src/util/api";

type HotRangesProps = RouteComponentProps<{ node_id: string }>;

const HotRanges = (props: HotRangesProps) => {
  const nodeIdParam = props.match.params["node_id"];
  const [nodeId, setNodeId] = useState(nodeIdParam);
  const [time, setTime] = useState<moment.Moment>(moment());
  const [hotRanges, setHotRanges] = useState<
    cockroach.server.serverpb.HotRangesResponseV2["ranges"]
  >([]);
  const [pageToken, setPageToken] = useState<string>("");
  const pageSize = 50;

  const refreshHotRanges = useCallback(() => {
    setHotRanges([]);
    setPageToken("");
  }, []);

  useEffect(() => {
    const request = cockroach.server.serverpb.HotRangesRequest.create({
      node_id: nodeId,
      page_size: pageSize,
      page_token: pageToken,
    });
    getHotRanges(request).then(response => {
      if (response.ranges.length == 0) {
        return;
      }
      setPageToken(response.next_page_token);
      setHotRanges([...hotRanges, ...response.ranges]);
      setTime(moment());
    });
    // Avoid dispatching request when `hotRanges` list is updated.
    // This effect should be triggered only when pageToken is changed.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pageToken]);

  useEffect(() => {
    setNodeId(nodeIdParam);
  }, [nodeIdParam]);
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
      }}
    >
      <span>{`Node ID: ${nodeId ?? "All nodes"}`}</span>
      <span>{`Time: ${time.toISOString()}`}</span>
      <Button onClick={refreshHotRanges} intent={"secondary"}>
        Refresh
      </Button>
      <pre className="state-json-box">{JSON.stringify(hotRanges, null, 2)}</pre>
    </div>
  );
};

export default withRouter(HotRanges);
