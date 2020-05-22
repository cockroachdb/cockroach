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
import { Trans, useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { Anchor, Tooltip } from "src/components";
import { statementDiagnostics, statementsRetries, statementsSql, statementsTimeInterval, transactionalPipelining } from "src/util/docs";
import getHighlightedText from "src/util/highlightedText";
import { summarize } from "src/util/sql/summarize";
import { ActivateDiagnosticsModalRef } from "./diagnostics/activateDiagnosticsModal";
import { DiagnosticStatusBadge } from "./diagnostics/diagnosticStatusBadge";
import { shortStatement } from "./statementsTable";

export type NodeNames = { [nodeId: string]: string };

const HeadTooltip = ({ i18nText, title }: { i18nText: string, title: any }) => {
  const { t } = useTranslation("common");
  return (
    <Tooltip
      placement="bottom"
      title={
        <div className="tooltip__table--title">
          {title}
        </div>
      }
    >
      {t(i18nText)}
    </Tooltip>
  );
};

const i18nKey = (value: string) => `common:statements.table.head.${value}`;

export const StatementTableTitles =  {
    statements: (
      <HeadTooltip
        i18nText="statements.table.head.statements.label"
        title={
          <Trans i18nKey={i18nKey("statements.tooltip")}>
            .<Anchor href={statementsSql} target="_blank">.</Anchor>.<br/><br/>.
          </Trans>
        }
      />
    ),
    txtType: (
      <HeadTooltip
        i18nText="statements.table.head.txn.label"
        title={
          <Trans i18nKey={i18nKey("txn.tooltip")}>
            .<code>.</code>.<code>.</code>.<Anchor href={transactionalPipelining} target="_blank">.</Anchor>.<br/><br/>.
          </Trans>
        }
      />
    ),
    diagnostics: (
      <HeadTooltip
        i18nText="statements.table.head.diagnostics.label"
        title={
          <Trans i18nKey={i18nKey("diagnostics.tooltip")}>
            .<Anchor href={statementDiagnostics} target="_blank">.</Anchor>.<code>.</code>.<code>.</code>.<code>.</code>.
          </Trans>
        }
      />
    ),
    retries: (
      <HeadTooltip
        i18nText="statements.table.head.retries.label"
        title={
          <Trans i18nKey={i18nKey("retries.tooltip")}>
            .<Anchor href={statementsRetries} target="_blank">.</Anchor>.
          </Trans>
        }
      />
    ),
    executionCount: (
      <HeadTooltip
        i18nText="statements.table.head.execution-count.label"
        title={
          <Trans i18nKey={i18nKey("execution-count.tooltip")}>
            .<Anchor href={statementsTimeInterval} target="_blank">.</Anchor>.<br/><br/>.<Anchor href={statementsRetries} target="_blank">.</Anchor>.
          </Trans>
        }
      />
    ),
    rowsAffected: (
      <HeadTooltip
        i18nText="statements.table.head.rows.label"
        title={
          <Trans i18nKey={i18nKey("rows.tooltip")}>
            .<Anchor href={statementsTimeInterval} target="_blank">.</Anchor>.<br/><br/>.
          </Trans>
        }
      />
    ),
    latency: (
      <HeadTooltip
        i18nText="statements.table.head.latency.label"
        title={
          <Trans i18nKey={i18nKey("latency.tooltip")}>
            .<br/><br/>.
          </Trans>
        }
      />
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

export const StatementLink = (props: { statement: string, app: string, implicitTxn: boolean, search: string }) => {
  const summary = summarize(props.statement);
  const base = props.app && props.app.length > 0 ? `/statements/${props.app}/${props.implicitTxn}` : `/statement/${props.implicitTxn}`;
  return (
    <Link to={ `${base}/${encodeURIComponent(props.statement)}` }>
      <div className="cl-table-link__tooltip">
        <Tooltip
          placement="bottom"
          title={<pre className="cl-table-link__description">
            { getHighlightedText(props.statement, props.search) }
          </pre>}
          overlayClassName="cl-table-link__statement-tooltip--fixed-width"
        >
          <div className="cl-table-link__tooltip-hover-area">
            { getHighlightedText(shortStatement(summary, props.statement), props.search, true) }
          </div>
        </Tooltip>
      </div>
    </Link>
  );
};

const NodeLink = (props: { nodeId: string, nodeNames: NodeNames }) => (
  <Link to={ `/node/${props.nodeId}` }>
    <div className="node-name-tooltip__info-icon">{props.nodeNames[props.nodeId]}</div>
  </Link>
);
