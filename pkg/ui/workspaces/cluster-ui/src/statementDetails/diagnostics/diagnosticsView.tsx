// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button, Icon, InlineAlert } from "@cockroachlabs/ui-components";
import classnames from "classnames/bind";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React from "react";
import { Link } from "react-router-dom";

import emptyListResultsImg from "src/assets/emptyState/empty-list-results.svg";
import { Button as CancelButton } from "src/button";
import { EmptyTable } from "src/empty";
import { ColumnDescriptor, SortedTable, SortSetting } from "src/sortedtable";
import {
  ActivateDiagnosticsModalRef,
  DiagnosticStatusBadge,
} from "src/statementsDiagnostics";
import { SummaryCard } from "src/summaryCard";
import {
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
} from "src/timeScaleDropdown";
import timeScaleStyles from "src/timeScaleDropdown/timeScale.module.scss";

import { StatementDiagnosticsReport, withBasePath } from "../../api";
import { FormattedTimescale } from "../../timeScaleDropdown/formattedTimeScale";
import { Timestamp } from "../../timestamp";
import { DATE_FORMAT_24_TZ } from "../../util";

import { filterByTimeScale, getDiagnosticsStatus } from "./diagnosticsUtils";
import styles from "./diagnosticsView.module.scss";

const timeScaleStylesCx = classNames.bind(timeScaleStyles);

export interface DiagnosticsViewStateProps {
  diagnosticsReports: StatementDiagnosticsReport[];
  showDiagnosticsViewLink?: boolean;
  activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>;
  currentScale: TimeScale;
  requestTime: moment.Moment;
}

export interface DiagnosticsViewDispatchProps {
  dismissAlertMessage: () => void;
  onDownloadDiagnosticBundleClick?: (statementFingerprint: string) => void;
  onDiagnosticCancelRequestClick?: (report: StatementDiagnosticsReport) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onChangeTimeScale: (ts: TimeScale) => void;
}

export interface DiagnosticsViewOwnProps {
  statementFingerprint?: string;
  planGists?: string[];
}

export type DiagnosticsViewProps = DiagnosticsViewOwnProps &
  DiagnosticsViewStateProps &
  DiagnosticsViewDispatchProps;

interface DiagnosticsViewState {
  sortSetting: SortSetting;
}

const cx = classnames.bind(styles);

const NavButton: React.FC = props => (
  <Button {...props} as="a" intent="tertiary">
    {props.children}
  </Button>
);

export const EmptyDiagnosticsView = ({
  statementFingerprint,
  planGists,
  showDiagnosticsViewLink,
  activateDiagnosticsRef,
}: DiagnosticsViewProps): React.ReactElement => {
  return (
    <EmptyTable
      icon={emptyListResultsImg}
      title="Activate statement diagnostics"
      footer={
        <footer className={cx("empty-view__footer")}>
          <Button
            intent="primary"
            onClick={() =>
              activateDiagnosticsRef?.current?.showModalFor(
                statementFingerprint,
                planGists,
              )
            }
          >
            Activate Diagnostics
          </Button>
          {showDiagnosticsViewLink && (
            <Link
              component={NavButton}
              to="/reports/statements/diagnosticshistory"
            >
              View all statement diagnostics
            </Link>
          )}
        </footer>
      }
    />
  );
};

const StmtDiagnosticLabel = ({
  currentScale,
  requestTime,
  undisplayedActiveReportExists,
}: {
  currentScale: TimeScale;
  requestTime: moment.Moment;
  undisplayedActiveReportExists: boolean;
}): JSX.Element => {
  return (
    <>
      <p className={timeScaleStylesCx("time-label", "label-margin")}>
        Showing statement diagnostics from{" "}
        <span className={timeScaleStylesCx("bold")}>
          <FormattedTimescale
            ts={currentScale}
            requestTime={moment(requestTime)}
          />
        </span>
      </p>
      {undisplayedActiveReportExists && (
        <InlineAlert
          intent="info"
          title={
            <>
              There is an active statement diagnostic request not displayed in
              the selected time window.
              <br />
              Further attempts to activate a diagnostic for this statement will
              fail.
            </>
          }
          className={cx("margin-bottom")}
        />
      )}
    </>
  );
};

export class DiagnosticsView extends React.Component<
  DiagnosticsViewProps,
  DiagnosticsViewState
> {
  constructor(props: DiagnosticsViewProps) {
    super(props);
    this.state = {
      sortSetting: {
        ascending: true,
        columnTitle: "activatedOn",
      },
    };
  }

  columns: ColumnDescriptor<StatementDiagnosticsReport>[] = [
    {
      name: "activatedOn",
      title: "Activated on",
      hideTitleUnderline: true,
      cell: (diagnostic: StatementDiagnosticsReport) => (
        <Timestamp time={diagnostic.requested_at} format={DATE_FORMAT_24_TZ} />
      ),
      sort: (diagnostic: StatementDiagnosticsReport) =>
        moment(diagnostic.requested_at)?.unix(),
    },
    {
      name: "status",
      title: "Status",
      hideTitleUnderline: true,
      className: cx("column-size-small"),
      cell: (diagnostic: StatementDiagnosticsReport) => {
        const status = getDiagnosticsStatus(diagnostic);
        return (
          <DiagnosticStatusBadge
            status={status}
            enableTooltip={status !== "READY"}
          />
        );
      },
      sort: (diagnostic: StatementDiagnosticsReport) =>
        String(diagnostic.completed),
    },
    {
      name: "actions",
      title: "",
      hideTitleUnderline: true,
      className: cx("column-size-medium"),
      cell: (diagnostic: StatementDiagnosticsReport) => {
        if (diagnostic.completed) {
          return (
            <div
              className={cx("crl-statements-diagnostics-view__actions-column")}
            >
              <Button
                as="a"
                size="small"
                intent="tertiary"
                href={withBasePath(
                  `_admin/v1/stmtbundle/${diagnostic.statement_diagnostics_id}`,
                )}
                onClick={() =>
                  this.props.onDownloadDiagnosticBundleClick &&
                  this.props.onDownloadDiagnosticBundleClick(
                    diagnostic.statement_fingerprint,
                  )
                }
                className={cx("download-bundle-button")}
              >
                <Icon iconName="Download" />
                Bundle (.zip)
              </Button>
            </div>
          );
        }
        return (
          <div
            className={cx("crl-statements-diagnostics-view__actions-column")}
          >
            <CancelButton
              size="small"
              type="secondary"
              onClick={() =>
                this.props.onDiagnosticCancelRequestClick &&
                this.props.onDiagnosticCancelRequestClick(diagnostic)
              }
            >
              Cancel request
            </CancelButton>
          </div>
        );
      },
      sort: (diagnostic: StatementDiagnosticsReport) =>
        String(diagnostic.completed),
    },
  ];

  componentWillUnmount(): void {
    this.props.dismissAlertMessage();
  }

  onSortingChange = (ss: SortSetting): void => {
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Diagnostics", ss.columnTitle, ss.ascending);
    }
    this.setState({
      sortSetting: {
        ascending: ss.ascending,
        columnTitle: ss.columnTitle,
      },
    });
  };

  render(): React.ReactElement {
    const {
      diagnosticsReports,
      showDiagnosticsViewLink,
      statementFingerprint,
      activateDiagnosticsRef,
      currentScale,
      onChangeTimeScale,
      planGists,
    } = this.props;

    const readyToRequestDiagnostics = diagnosticsReports.every(
      diagnostic => diagnostic.completed,
    );

    // Get diagnostic reports within the time window.
    const dataSource = filterByTimeScale(
      diagnosticsReports.map((diagnosticsReport, idx) => ({
        ...diagnosticsReport,
        key: idx,
      })),
      currentScale,
    );

    // Get active report not within the time window if exists.
    const undisplayedActiveReportExists: boolean =
      diagnosticsReports.findIndex(
        report =>
          !report.completed && !dataSource.find(data => data.id === report.id),
      ) !== -1;

    if (dataSource.length === 0) {
      return (
        <>
          <TimeScaleDropdown
            options={timeScale1hMinOptions}
            currentScale={currentScale}
            setTimeScale={onChangeTimeScale}
            className={cx("timescale-small", "margin-bottom")}
          />
          <StmtDiagnosticLabel
            currentScale={this.props.currentScale}
            requestTime={moment(this.props.requestTime)}
            undisplayedActiveReportExists={undisplayedActiveReportExists}
          />
          <SummaryCard>
            <EmptyDiagnosticsView {...this.props} />
          </SummaryCard>
        </>
      );
    }

    return (
      <>
        <div className={cx("crl-statements-diagnostics-view__header")}>
          <TimeScaleDropdown
            options={timeScale1hMinOptions}
            currentScale={currentScale}
            setTimeScale={onChangeTimeScale}
            className={cx("timescale-small")}
          />
          {readyToRequestDiagnostics && (
            <Button
              onClick={() =>
                activateDiagnosticsRef?.current?.showModalFor(
                  statementFingerprint,
                  planGists,
                )
              }
              disabled={!readyToRequestDiagnostics}
              intent="secondary"
            >
              Activate diagnostics
            </Button>
          )}
        </div>
        <StmtDiagnosticLabel
          currentScale={this.props.currentScale}
          requestTime={moment(this.props.requestTime)}
          undisplayedActiveReportExists={undisplayedActiveReportExists}
        />
        <SortedTable
          data={dataSource}
          columns={this.columns}
          className={cx("jobs-table")}
          sortSetting={this.state.sortSetting}
          onChangeSortSetting={this.onSortingChange}
          tableWrapperClassName={cx("sorted-table")}
        />
        {showDiagnosticsViewLink && (
          <div className={cx("crl-statements-diagnostics-view__footer")}>
            <Link
              component={NavButton}
              to="/reports/statements/diagnosticshistory"
            >
              All statement diagnostics
            </Link>
          </div>
        )}
      </>
    );
  }
}
