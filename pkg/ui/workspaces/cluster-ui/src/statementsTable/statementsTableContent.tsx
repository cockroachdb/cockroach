// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { EllipsisVertical } from "@cockroachlabs/icons";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import noop from "lodash/noop";
import moment from "moment-timezone";
import React from "react";
import { Link } from "react-router-dom";

import { withBasePath } from "src/api/basePath";
import { StatementDiagnosticsReport } from "src/api/statementDiagnosticsApi";
import { Button } from "src/button";
import { Dropdown } from "src/dropdown";
import { getHighlightedText } from "src/highlightedText";
import {
  ActivateDiagnosticsModalRef,
  DiagnosticStatusBadge,
} from "src/statementsDiagnostics";
import { AggregateStatistics } from "src/statementsTable";
import {
  propsToQueryString,
  computeOrUseStmtSummary,
  appNamesAttr,
  unset,
} from "src/util";

import styles from "./statementsTableContent.module.scss";

export type NodeNames = { [nodeId: string]: string };
const cx = classNames.bind(styles);

export const StatementTableCell = {
  statements:
    (
      search?: string,
      selectedApp?: string[],
      onStatementClick?: (statement: string) => void,
    ) =>
    (stmt: AggregateStatistics): React.ReactElement => (
      <StatementLink
        statementFingerprintID={stmt.aggregatedFingerprintID}
        statement={stmt.label}
        statementSummary={stmt.summary}
        aggregatedTs={stmt.aggregatedTs}
        appNames={[
          stmt.applicationName != null
            ? stmt.applicationName
              ? stmt.applicationName
              : unset
            : null,
        ]}
        implicitTxn={stmt.implicitTxn}
        search={search}
        onClick={onStatementClick}
      />
    ),
  diagnostics:
    (
      activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>,
      onSelectDiagnosticsReportDropdownOption: (
        report: StatementDiagnosticsReport,
      ) => void = noop,
    ) =>
    (stmt: AggregateStatistics): React.ReactElement => {
      /*
       * Diagnostics cell might display different components depending
       * on following states:
       * - show `Activate` link only if no completed or waiting reports available;
       * - show `WAITING` badge and ellipsis button with option for diagnostics
       * cancellation if report requested for the first time;
       * - show `WAITING` badge and ellipsis button with options for diagnostics
       * cancellation and download links if a report is currently requested and previous
       * completed reports are available for download;
       * - show `Activate` link and ellipsis button with download links if there are completed
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
                activateDiagnosticsRef?.current?.showModalFor(
                  stmt.label,
                  stmt.stats.plan_gists,
                )
              }
              type="secondary"
              size="small"
            >
              Activate
            </Button>
          ) : (
            <DiagnosticStatusBadge status="WAITING" />
          )}
          {(!canActivateDiagnosticReport || hasCompletedDiagnosticsReports) && (
            <Dropdown<StatementDiagnosticsReport>
              items={stmt.diagnosticsReports
                // Sort diagnostic reports from incomplete to complete. Incomplete reports are cancellable.
                .sort(function (a, b) {
                  if (a.completed === b.completed) {
                    return 0;
                  }
                  return a.completed ? 1 : -1;
                })
                .map(dr => {
                  // If diagnostic report is not complete (i.e. waiting) create an option to cancel it.
                  if (!dr.completed) {
                    return {
                      name: (
                        <div
                          className={cx("diagnostic-report-dropdown-option")}
                        >
                          {`Cancel diagnostic request`}
                        </div>
                      ),
                      value: dr,
                    };
                  }
                  // Diagnostic report is complete, create an option to download.
                  else {
                    return {
                      name: (
                        <a
                          className={cx("diagnostic-report-dropdown-option")}
                          href={withBasePath(
                            `_admin/v1/stmtbundle/${dr.statement_diagnostics_id}`,
                          )}
                        >
                          {`Download ${moment(dr.requested_at).format(
                            "MMM DD, YYYY [at] H:mm [(UTC)] [diagnostic]",
                          )}`}
                        </a>
                      ),
                      value: dr,
                    };
                  }
                })}
              onChange={onSelectDiagnosticsReportDropdownOption}
              menuPosition="right"
              customToggleButtonOptions={{
                size: "small",
                icon: <EllipsisVertical />,
                textAlign: "center",
              }}
              className={cx("activate-diagnostic-dropdown")}
            />
          )}
        </div>
      );
    },
};

type StatementLinkTargetProps = {
  statementFingerprintID: string;
  aggregatedTs?: number;
  appNames?: string[];
  implicitTxn: boolean;
};

// StatementLinkTarget returns the link to the relevant statement page, given
// the input statement details.
export const StatementLinkTarget = (
  props: StatementLinkTargetProps,
): string => {
  const base = `/statement/${props.implicitTxn}`;
  const statementFingerprintID = props.statementFingerprintID;

  const searchParams = propsToQueryString({
    [appNamesAttr]: props.appNames,
  });

  return `${base}/${encodeURIComponent(
    statementFingerprintID,
  )}?${searchParams}`;
};

interface StatementLinkProps {
  statementFingerprintID: string;
  aggregatedTs?: number;
  appNames?: string[];
  implicitTxn: boolean;
  statement: string;
  statementSummary: string;
  search?: string;
  statementQuery?: string;
  onClick?: (statement: string) => void;
  className?: string;
}

export const StatementLink = ({
  statementFingerprintID,
  appNames,
  implicitTxn,
  statement,
  statementSummary,
  search,
  onClick,
  className,
}: StatementLinkProps): React.ReactElement => {
  const onStatementClick = React.useCallback(() => {
    if (onClick) {
      onClick(statement);
    }
  }, [onClick, statement]);

  const linkProps = {
    statementFingerprintID,
    appNames,
    implicitTxn,
  };

  const summary = computeOrUseStmtSummary(statement, statementSummary);

  return (
    <Link
      to={StatementLinkTarget(linkProps)}
      onClick={onStatementClick}
      className={`${cx(className)}`}
    >
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
            {getHighlightedText(summary, search, false, true)}
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
    <div className={cx("node-link")}>
      {props.nodeNames ? props.nodeNames[props.nodeId] : "N" + props.nodeId}
    </div>
  </Link>
);
