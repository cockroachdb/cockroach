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
  const requestHotRanges = useCallback(() => {
    const request = cockroach.server.serverpb.HotRangesRequest.create({
      node_id: nodeId,
    });
    getHotRanges(request).then(response => {
      setHotRanges(response.ranges);
      setTime(moment());
    });
  }, [nodeId]);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(requestHotRanges, [nodeId]);
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
      <Button onClick={requestHotRanges} intent={"secondary"}>
        Refresh
      </Button>
      <pre className="state-json-box">{JSON.stringify(hotRanges, null, 2)}</pre>
    </div>
  );
};

export default withRouter(HotRanges);
