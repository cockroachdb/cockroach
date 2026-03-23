// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  SortedTable,
  util,
  Timestamp,
  useNodesSummary,
  useNodeLogs,
} from "@cockroachlabs/cluster-ui";
import sortBy from "lodash/sortBy";
import React from "react";
import { Helmet } from "react-helmet";
import { useRouteMatch } from "react-router-dom";

import * as protos from "src/js/protos";
import { nodeIDAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import "./logs.scss";

type LogEntries = protos.cockroach.util.log.IEntry;

/**
 * Renders the main content of the logs page.
 */
export function Logs(): React.ReactElement {
  const match = useRouteMatch();
  const nodeId = getMatchParamByName(match, nodeIDAttr);

  const { nodeDisplayNameByID, nodeStatusByID } = useNodesSummary();
  const {
    data: logs,
    isLoading: logsLoading,
    error: logsError,
  } = useNodeLogs(nodeId);

  const node = nodeStatusByID?.[nodeId];

  const renderContent = () => {
    const logEntries = sortBy(logs.entries, e => e.time);
    const columns = [
      {
        title: "Time",
        name: "time",
        cell: (logEntry: LogEntries) => (
          <Timestamp
            time={util.LongToMoment(logEntry.time)}
            format={util.DATE_WITH_SECONDS_FORMAT_24_TZ}
          />
        ),
      },
      {
        title: "Severity",
        name: "severity",
        cell: (logEntry: LogEntries) =>
          protos.cockroach.util.log.Severity[logEntry.severity],
      },
      {
        title: "Message",
        name: "message",
        cell: (logEntry: LogEntries) => (
          <pre className="sort-table__unbounded-column logs-table__message">
            {(logEntry.tags ? "[" + logEntry.tags + "] " : "") +
              logEntry.message}
          </pre>
        ),
      },
      {
        title: "File:Line",
        name: "file",
        cell: (logEntry: LogEntries) => `${logEntry.file}:${logEntry.line}`,
      },
    ];
    return (
      <SortedTable data={logEntries} columns={columns} className="logs-table" />
    );
  };

  const nodeAddress = node ? node.desc.address.address_field : null;
  const title = node
    ? `Logs | ${nodeDisplayNameByID[nodeId]} | Nodes`
    : `Logs | Node ${nodeId} | Nodes`;

  return (
    <div>
      <Helmet title={title} />
      <div className="section section--heading">
        <h2 className="base-heading">
          Logs Node {nodeId} / {nodeAddress}
        </h2>
      </div>
      <section className="section">
        <Loading
          loading={logsLoading || !logs}
          page={"node logs"}
          error={logsError}
          render={renderContent}
        />
      </section>
    </div>
  );
}

export default Logs;
