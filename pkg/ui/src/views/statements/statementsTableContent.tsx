// Copyright 2020 The Cockroach Authors.
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
import { Anchor, Tooltip } from "src/components";
import { statementDiagnostics, statementsRetries, statementsSql, statementsTimeInterval, transactionalPipelining } from "src/util/docs";
import getHighlightedText from "src/util/highlightedText";
import { summarize } from "src/util/sql/summarize";
import { ActivateDiagnosticsModalRef } from "./diagnostics/activateDiagnosticsModal";
import { DiagnosticStatusBadge } from "./diagnostics/diagnosticStatusBadge";
import { shortStatement } from "./statementsTable";
import styles from "./statementsTableContent.module.styl";

export type NodeNames = { [nodeId: string]: string };

const cx = classNames.bind(styles);

export const StatementTableTitle = {
  statements: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"SQL statement "}
            <Anchor
              href={statementsSql}
              target="_blank"
            >
              fingerprint.
            </Anchor>
          </p>
          <p>
            To view additional details of a SQL statement fingerprint, click this to open the Statement Details page.
          </p>
        </div>
      }
    >
      Statements
    </Tooltip>
  ),
  txtType: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Type of transaction (implicit or explicit). Explicit transactions refer to statements that are wrapped by "}
            <code>BEGIN</code>
            {" and "}
            <code>COMMIT</code>
            {" statements by the client. Explicit transactions employ "}
            <Anchor
              href={transactionalPipelining}
              target="_blank"
            >
              transactional pipelining
            </Anchor>
            {" and therefore report latencies that do not account for replication."}
          </p>
          <p>
            For statements not in explicit transactions, CockroachDB wraps each statement in individual implicit transactions.
          </p>
        </div>
      }
    >
      TXN Type
    </Tooltip>
  ),
  diagnostics: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Option to activate "}
            <Anchor
              href={statementDiagnostics}
              target="_blank"
            >
              diagnostics
            </Anchor>
            {" for each statement. If activated, this displays the status of diagnostics collection ("}
            <code>WAITING FOR QUERY</code>, <code>READY</code>, OR <code>ERROR</code>).
          </p>
        </div>
      }
    >
      Diagnostics
    </Tooltip>
  ),
  retries: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Cumulative number of "}
            <Anchor
              href={statementsRetries}
              target="_blank"
            >
              retries
            </Anchor>
            {" of statements with this fingerprint within the last hour or specified time interval."}
          </p>
        </div>
      }
    >
      Retries
    </Tooltip>
  ),
  executionCount: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Cumulative number of executions of statements with this fingerprint within the last hour or specified "}
            <Anchor
              href={statementsTimeInterval}
              target="_blank"
            >
              time interval
            </Anchor>.
          </p>
          <p>
            {"The bar indicates the ratio of runtime success (gray) to "}
            <Anchor
              href={statementsRetries}
              target="_blank"
            >
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
  rowsAffected: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Average number of rows returned while executing statements with this fingerprint within the last hour or specified "}
            <Anchor
              href={statementsTimeInterval}
              target="_blank"
            >
              time interval
            </Anchor>.
          </p>
          <p>
            The gray bar indicates the mean number of rows returned. The blue bar indicates one standard deviation from the mean.
          </p>
        </div>
      }
    >
      Rows Affected
    </Tooltip>
  ),
  latency: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            Average service latency of statements with this fingerprint within the last hour or specified time interval.
          </p>
          <p>
            The gray bar indicates the mean latency. The blue bar indicates one standard deviation from the mean.
          </p>
        </div>
      }
    >
      Latency
    </Tooltip>
  ),
};

export const StatementTableCell = {
  statements: (search?: string, selectedApp?: string) => (stmt: any) => (
    <StatementLink
      statement={ stmt.label }
      implicitTxn={ stmt.implicitTxn }
      search={search}
      app={ selectedApp }
    />
  ),
  diagnostics: (activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>) => (stmt: any) => {
    if (stmt.diagnosticsReport) {
      return <DiagnosticStatusBadge status={stmt.diagnosticsReport.completed ? "READY" : "WAITING FOR QUERY"}/>;
    }
    return (
      <Anchor
        onClick={() => activateDiagnosticsRef?.current?.showModalFor(stmt.label)}
      >
        Activate
      </Anchor>
    );
  },
  nodeLink: (nodeNames: NodeNames) => (stmt: any) => <NodeLink nodeId={stmt.label} nodeNames={ nodeNames } />,
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
    <Link to={ StatementLinkTarget(props) }>
      <div>
        <Tooltip
          placement="bottom"
          title={<pre className={cx("cl-table-link__description")}>
            { getHighlightedText(props.statement, props.search) }
          </pre>}
          overlayClassName={cx("cl-table-link__statement-tooltip--fixed-width")}
        >
          <div className="cl-table-link__tooltip-hover-area">
            { getHighlightedText(shortStatement(summary, props.statement), props.search, true) }
          </div>
        </Tooltip>
      </div>
    </Link>
  );
};

export const NodeLink = (props: { nodeId: string, nodeNames: NodeNames }) => (
  <Link to={ `/node/${props.nodeId}` }>
    <span className="node-name-tooltip__info-icon">{props.nodeNames[props.nodeId]}</span>
  </Link>
);
