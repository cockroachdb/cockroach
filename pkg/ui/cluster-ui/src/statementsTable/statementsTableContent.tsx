import React from "react";
import { Link } from "react-router-dom";
import classNames from "classnames/bind";
import { noop } from "lodash";
import { Anchor } from "src/anchor";
import {
  ActivateDiagnosticsModalRef,
  DiagnosticStatusBadge,
} from "src/statementsDiagnostics";
import { getHighlightedText } from "src/highlightedText";
import { AggregateStatistics } from "src/statementsTable";
import { Dropdown } from "src/dropdown";
import { Button } from "src/button";

import { Tooltip2 as Tooltip } from "src/tooltip2";
import {
  statementDiagnostics,
  statementsRetries,
  statementsSql,
  statementsTimeInterval,
  readFromDisk,
  planningExecutionTime,
  contentionTime,
  readsAndWrites,
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

export const StatementTableTitle = {
  statements: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"SQL statement "}
            <Anchor href={statementsSql} target="_blank">
              fingerprint.
            </Anchor>
          </p>
          <p>
            To view additional details of a SQL statement fingerprint, click
            this to open the Statement Details page.
          </p>
        </div>
      }
    >
      Statements
    </Tooltip>
  ),
  executionCount: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {
              "Cumulative number of executions of statements with this fingerprint within the last hour or specified "
            }
            <Anchor href={statementsTimeInterval} target="_blank">
              time interval
            </Anchor>
            .
          </p>
          <p>
            {"The bar indicates the ratio of runtime success (gray) to "}
            <Anchor href={statementsRetries} target="_blank">
              retries
            </Anchor>
            {" (red) for the SQL statement fingerprint."}
          </p>
        </div>
      }
    >
      Execution Count
    </Tooltip>
  ),
  rowsRead: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Aggregation of all rows "}
            <Anchor href={readFromDisk} target="_blank">
              read from disk
            </Anchor>
            {
              " across all operators for statements with this fingerprint within the last hour or specified "
            }
            <Anchor href={statementsTimeInterval} target="_blank">
              time interval
            </Anchor>
            .
          </p>
          <p>
            The gray bar indicates the mean number of rows read from disk. The
            blue bar indicates one standard deviation from the mean.
          </p>
        </div>
      }
    >
      Rows Read
    </Tooltip>
  ),
  bytesRead: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Aggregation of all bytes "}
            <Anchor href={readFromDisk} target="_blank">
              read from disk
            </Anchor>
            {
              " across all operators for statements with this fingerprint within the last hour or specified "
            }
            <Anchor href={statementsTimeInterval} target="_blank">
              time interval
            </Anchor>
            .
          </p>
          <p>
            The gray bar indicates the mean number of bytes read from disk. The
            blue bar indicates one standard deviation from the mean.
          </p>
        </div>
      }
    >
      Bytes Read
    </Tooltip>
  ),
  statementTime: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Average "}
            <Anchor href={planningExecutionTime} target="_blank">
              planning and execution time
            </Anchor>
            {
              " of statements with this fingerprint within the last hour or specified time interval."
            }
          </p>
          <p>
            The gray bar indicates the mean latency. The blue bar indicates one
            standard deviation from the mean.
          </p>
        </div>
      }
    >
      Statement Time
    </Tooltip>
  ),
  transactionTime: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Average "}
            <Anchor href={planningExecutionTime} target="_blank">
              planning and execution time
            </Anchor>
            {
              " of transactions with this fingerprint within the last hour or specified time interval."
            }
          </p>
          <p>
            The gray bar indicates the mean latency. The blue bar indicates one
            standard deviation from the mean.
          </p>
        </div>
      }
    >
      Transaction Time
    </Tooltip>
  ),
  contention: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Average time statements with this fingerprint were "}
            <Anchor href={contentionTime} target="_blank">
              in contention
            </Anchor>
            {" with other transactions within the last hour or specified "}
            <Anchor href={statementsTimeInterval} target="_blank">
              time interval
            </Anchor>
            .
          </p>
          <p>
            The gray bar indicates mean contention time. The blue bar indicates
            one standard deviation from the mean.
          </p>
        </div>
      }
    >
      Contention
    </Tooltip>
  ),
  maxMemUsage: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {
              "Maximum memory used by a statement with this fingerprint at any time during its execution within the last hour or specified "
            }
            <Anchor href={statementsTimeInterval} target="_blank">
              time interval
            </Anchor>
            .
          </p>
          <p>
            The gray bar indicates the average max memory usage. The blue bar
            indicates one standard deviation from the mean.
          </p>
        </div>
      }
    >
      Max Memory
    </Tooltip>
  ),
  networkBytes: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Amount of data "}
            <Anchor href={readsAndWrites} target="_blank">
              data transferred over the network
            </Anchor>
            {
              " (e.g., between regions and nodes) for statements with this fingerprint within the last hour or speicifed "
            }
            <Anchor href={statementsTimeInterval} target="_blank">
              time interval
            </Anchor>
            .
          </p>
          <p>
            If this value is 0, the statement was executed on a single node.
          </p>
          <p>
            The gray bar indicates the mean number of bytes sent over the
            network. The blue bar indicates one standard deviation from the
            mean.
          </p>
        </div>
      }
    >
      Network
    </Tooltip>
  ),
  retries: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Cumulative number of "}
            <Anchor href={statementsRetries} target="_blank">
              retries
            </Anchor>
            {
              " of statements with this fingerprint within the last hour or specified time interval."
            }
          </p>
        </div>
      }
    >
      Retries
    </Tooltip>
  ),
  workloadPct: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            % of runtime all statements with this fingerprint represent,
            compared to the cumulative runtime of all queries within the last
            hour or specified time interval.
          </p>
        </div>
      }
    >
      % of all runtime
    </Tooltip>
  ),
  diagnostics: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Option to activate "}
            <Anchor href={statementDiagnostics} target="_blank">
              diagnostics
            </Anchor>
            {
              " for each statement. If activated, this displays the status of diagnostics collection ("
            }
            <code>WAITING</code>, <code>READY</code>, OR <code>ERROR</code>).
          </p>
        </div>
      }
    >
      Diagnostics
    </Tooltip>
  ),
};

export const StatementTableCell = {
  statements: (search?: string, selectedApp?: string) => (stmt: any) => (
    <StatementLink
      statement={stmt.label}
      implicitTxn={stmt.implicitTxn}
      search={search}
      app={selectedApp}
    />
  ),
  diagnostics: (
    activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>,
    onDiagnosticsDownload: (report: IStatementDiagnosticsReport) => void = noop,
  ) => (stmt: AggregateStatistics) => {
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
  nodeLink: (nodeNames: NodeNames) => (stmt: any) => (
    <NodeLink nodeId={stmt.label} nodeNames={nodeNames} />
  ),
};

interface StatementLinkProps {
  statement: string;
  app: string;
  implicitTxn: boolean;
  search: string;
  anonStatement?: string;
}

// StatementLinkTarget returns the link to the relevant statement page, given
// the input statement details.
export const StatementLinkTarget = (props: StatementLinkProps) => {
  let base: string;
  if (props.app && props.app.length > 0) {
    base = `/statements/${props.app}/${props.implicitTxn}`;
  } else {
    base = `/statement/${props.implicitTxn}`;
  }

  let linkStatement = props.statement;
  if (props.anonStatement) {
    linkStatement = props.anonStatement;
  }
  return `${base}/${encodeURIComponent(linkStatement)}`;
};

export const StatementLink = (props: StatementLinkProps) => {
  const summary = summarize(props.statement);
  return (
    <Link to={StatementLinkTarget(props)}>
      <div>
        <Tooltip
          placement="bottom"
          title={
            <pre className={cx("cl-table-link__description")}>
              {getHighlightedText(props.statement, props.search)}
            </pre>
          }
          overlayClassName={cx("cl-table-link__statement-tooltip--fixed-width")}
        >
          <div className="cl-table-link__tooltip-hover-area">
            {getHighlightedText(
              shortStatement(summary, props.statement),
              props.search,
              true,
            )}
          </div>
        </Tooltip>
      </div>
    </Link>
  );
};

export const NodeLink = (props: { nodeId: string; nodeNames: NodeNames }) => (
  <Link to={`/node/${props.nodeId}`}>
    <div className={cx("node-name-tooltip__info-icon")}>
      {props.nodeNames[props.nodeId]}
    </div>
  </Link>
);
