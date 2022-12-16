// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { GraphData, GraphNode, GraphLink  } from "react-d3-graph";
import { ContentionEventsResponse } from "../api/txnContentionApi";

export function mapContentionToGraphData (contentionEvents: ContentionEventsResponse): GraphData<GraphNode, GraphLink> {
  const contentionNodes: GraphNode[] = [];
  const contentionLinks: GraphLink[] = [];

  contentionEvents?.forEach(event => {
      contentionNodes.push({id: event.waitingTxnExecutionID});
      contentionLinks.push({source: event.waitingTxnExecutionID, target: event.blockingTxnExecutionID});
    }
  )

  const data: GraphData<GraphNode, GraphLink> = {
    nodes: contentionNodes,
    links: contentionLinks,
  }

  return data
}
