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
import { Link } from "react-router-dom";
import classNames from "classnames/bind";
import { noop } from "lodash";
import {
  ActivateDiagnosticsModalRef,
  DiagnosticStatusBadge,
} from "src/statementsDiagnostics";
import { getHighlightedText } from "src/highlightedText";
import { AggregateStatistics } from "src/statementsTable";
import { Dropdown } from "src/dropdown";
import { Button } from "src/button";

import { Tooltip } from "@cockroachlabs/ui-components";
import {
  appAttr,
  databaseAttr,
  aggregatedTsAttr,
  propsToQueryString,
  summarize,
  TimestampToMoment,
} from "src/util";
import { shortStatement } from "./statementsTable";
import styles from "./statementsTableContent.module.scss";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Download } from "@cockroachlabs/icons";
import { getBasePath } from "../api";

export type NodeNames = { [nodeId: string]: string };
const cx = classNames.bind(styles);
type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

export const StatementTableCell = {
  statements: (
    search?: string,
    selectedApp?: string,
    onStatementClick?: (statement: string) => void,
  ) => (stmt: AggregateStatistics): React.ReactElement => (
    <StatementLink
      statement={stmt.label}
      aggregatedTs={stmt.aggregatedTs}
      database={stmt.database}
      implicitTxn={stmt.implicitTxn}
      search={search}
      app={selectedApp}
      onClick={onStatementClick}
    />
  ),
  diagnostics: (
    activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>,
    onDiagnosticsDownload: (report: IStatementDiagnosticsReport) => void = noop,
  ) => (stmt: AggregateStatistics): React.ReactElement => {
    /*
     * Diagnostics cell might display different components depending
     * on following states:
     * - show `Activate` link only if no completed or waiting reports available;
     * - show `WAITING` badge only if report requested for the first time;
     * - show `WAITING` badge and dropdown with download links if report is currently
     * requested and previous reports are available for download;
     * - show `Activate` link and dropdown with download links if there is completed
     * reports only;
     * */
    const hasDiagnosticReports = !!stmt.diagnosticsReports;
    const hasCompletedDiagnosticsReports =
      hasDiagnosticReports && stmt.diagnosticsReports.some(d => d.completed);
    const canActivateDiagnosticReport = hasDiagnosticReports
      ? stmt.diagnosticsReports.every(d => d.completed)
      : true;

    return (
      <div className={cx("activate-diagnostic-col")}>
        {canActivateDiagnosticReport ? (
          <Button
            onClick={() =>
              activateDiagnosticsRef?.current?.showModalFor(stmt.label)
            }
            type="secondary"
            size="small"
          >
            Activate
          </Button>
        ) : (
          <DiagnosticStatusBadge status="WAITING" />
        )}
        {hasCompletedDiagnosticsReports && (
          <Dropdown<IStatementDiagnosticsReport>
            items={stmt.diagnosticsReports
              .filter(dr => dr.completed)
              .map(dr => ({
                name: (
                  <a
                    className={cx("download-diagnostics-link")}
                    href={`${getBasePath()}/_admin/v1/stmtbundle/${
                      dr.statement_diagnostics_id
                    }`}
                  >
                    {`${TimestampToMoment(dr.requested_at).format(
                      "ll [at] LT [diagnostic]",
                    )}`}
                  </a>
                ),
                value: dr,
              }))}
            onChange={onDiagnosticsDownload}
            menuPosition="right"
            customToggleButtonOptions={{
              size: "small",
              icon: <Download />,
              textAlign: "center",
            }}
            className={cx("activate-diagnostic-dropdown")}
          />
        )}
      </div>
    );
  },
  nodeLink: (nodeNames: NodeNames) => (
    stmt: AggregateStatistics,
  ): React.ReactElement => (
    <NodeLink nodeId={stmt.label} nodeNames={nodeNames} />
  ),
};

type StatementLinkTargetProps = {
  statement: string;
  aggregatedTs?: number;
  app: string;
  implicitTxn: boolean;
  statementNoConstants?: string;
  database?: string;
};

// StatementLinkTarget returns the link to the relevant statement page, given
// the input statement details.
export const StatementLinkTarget = (
  props: StatementLinkTargetProps,
): string => {
  const base = `/statement/${props.implicitTxn}`;
  const linkStatement = props.statementNoConstants || props.statement;

  const searchParams = propsToQueryString({
    [databaseAttr]: props.database,
    [appAttr]: props.app,
    [aggregatedTsAttr]: props.aggregatedTs,
  });

  return `${base}/${encodeURIComponent(linkStatement)}?${searchParams}`;
};

interface StatementLinkProps {
  aggregatedTs?: number;
  statement: string;
  app: string;
  implicitTxn: boolean;
  search: string;
  statementNoConstants?: string;
  database?: string;
  onClick?: (statement: string) => void;
}

export const StatementLink = ({
  aggregatedTs,
  statement,
  app,
  implicitTxn,
  search,
  statementNoConstants,
  database,
  onClick,
}: StatementLinkProps): React.ReactElement => {
  const summary = summarize(statement);
  const onStatementClick = React.useCallback(() => {
    if (onClick) {
      onClick(statement);
    }
  }, [onClick, statement]);

  const linkProps = {
    aggregatedTs,
    statement,
    app,
    implicitTxn,
    statementNoConstants,
    database,
  };

  return (
    <Link to={StatementLinkTarget(linkProps)} onClick={onStatementClick}>
      <div>
        <Tooltip
          placement="bottom"
          content={
            <pre className={cx("cl-table-link__description")}>
              {getHighlightedText(statement, search, true)}
            </pre>
          }
        >
          <div className="cl-table-link__tooltip-hover-area">
            {getHighlightedText(
              shortStatement(summary, statement),
              search,
              false,
              true,
            )}
          </div>
        </Tooltip>
      </div>
    </Link>
  );
};

export const NodeLink = (props: {
  nodeId: string;
  nodeNames?: NodeNames;
}): React.ReactElement => (
  <Link to={`/node/${props.nodeId}`}>
    <div className={cx("node-name-tooltip__info-icon")}>
      {props.nodeNames ? props.nodeNames[props.nodeId] : "N" + props.nodeId}
    </div>
  </Link>
);
