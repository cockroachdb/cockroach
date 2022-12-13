// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Graph, GraphData, GraphLink, GraphNode } from "react-d3-graph";
import { mapContentionToGraphData } from "./utils";
import React, { useEffect } from "react";
import { ContentionEventsResponse } from "../api/txnContentionApi";

type ContentionGraphProps = {
  contentionEvents: ContentionEventsResponse;
};

export const ContentionGraph: React.FC<ContentionGraphProps> = ({
                                                                  contentionEvents
                                                                 }) => {

  const onClickNode = function(nodeId: string) {
    window.alert(`Clicked node ${nodeId}`);
  };

  const onClickLink = function(source: string, target: string) {
    window.alert(`Clicked link between ${source} and ${target}`);
  };

  let data: GraphData<GraphNode, GraphLink>;

  useEffect( () => {
      if (!contentionEvents) {
        console.log("no events")
      }
      data = mapContentionToGraphData(contentionEvents);
    }
  , [contentionEvents])

  return (
    <Graph
      id="txn-contention" // id is mandatory
      data={data}
      onClickNode={onClickNode}
      onClickLink={onClickLink}
    />
  );
};
