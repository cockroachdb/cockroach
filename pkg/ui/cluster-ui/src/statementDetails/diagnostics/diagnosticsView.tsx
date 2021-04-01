import React from "react";
import { Link } from "react-router-dom";
import moment from "moment";
import classnames from "classnames/bind";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Download } from "@cockroachlabs/icons";
import { Button } from "src/button";
import { Text, TextTypes } from "src/text";
import { Table, ColumnsConfig } from "src/table";
import { SummaryCard } from "src/summaryCard";
import { DiagnosticStatusBadge } from "src/statementsDiagnostics";
import emptyTracingBackground from "src/assets/statementsPage/emptyTracingBackground.svg";
import {
  getDiagnosticsStatus,
  sortByCompletedField,
  sortByRequestedAtField,
} from "./diagnosticsUtils";
import { statementDiagnostics } from "src/util/docs";
import { EmptyPanel } from "src/empty";
import styles from "./diagnosticsView.module.scss";
import { getBasePath } from "../../api";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

export interface DiagnosticsViewStateProps {
  hasData: boolean;
  diagnosticsReports: cockroach.server.serverpb.IStatementDiagnosticsReport[];
  showDiagnosticsViewLink?: boolean;
}

export interface DiagnosticsViewDispatchProps {
  activate: (statementFingerprint: string) => void;
  dismissAlertMessage: () => void;
  onDownloadDiagnosticBundleClick?: (statementFingerprint: string) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
}

export interface DiagnosticsViewOwnProps {
  statementFingerprint?: string;
}

export type DiagnosticsViewProps = DiagnosticsViewOwnProps &
  DiagnosticsViewStateProps &
  DiagnosticsViewDispatchProps;

interface DiagnosticsViewState {
  traces: {
    [diagnosticsId: string]: string;
  };
}

const cx = classnames.bind(styles);

export const EmptyDiagnosticsView = ({
  activate,
  statementFingerprint,
}: DiagnosticsViewProps) => {
  const onActivateButtonClick = () => {
    activate(statementFingerprint);
  };
  return (
    <EmptyPanel
      title="Activate statement diagnostics"
      description="When you activate statement diagnostics, CockroachDB will wait for the next query that matches
      this statement fingerprint. A download button will appear on the statement list and detail pages
      when the query is ready. The statement diagnostic will include EXPLAIN plans,
      table statistics, and traces."
      anchor="Learn More"
      link={statementDiagnostics}
      label="Activate"
      onClick={onActivateButtonClick}
      backgroundImage={emptyTracingBackground}
    />
  );
};

export class DiagnosticsView extends React.Component<
  DiagnosticsViewProps,
  DiagnosticsViewState
> {
  static defaultProps: Partial<DiagnosticsViewProps> = {
    showDiagnosticsViewLink: true,
  };
  columns: ColumnsConfig<IStatementDiagnosticsReport> = [
    {
      key: "activatedOn",
      title: "Activated on",
      sorter: sortByRequestedAtField,
      defaultSortOrder: "descend",
      render: (_text, record) => {
        const timestamp = record.requested_at.seconds.toNumber() * 1000;
        return moment(timestamp).format("LL[ at ]h:mm a");
      },
    },
    {
      key: "status",
      title: "status",
      sorter: sortByCompletedField,
      width: "160px",
      render: (_text, record) => {
        const status = getDiagnosticsStatus(record);
        return (
          <DiagnosticStatusBadge
            status={status}
            enableTooltip={status !== "READY"}
          />
        );
      },
    },
    {
      key: "actions",
      title: "",
      sorter: false,
      width: "160px",
      render: ((onDownloadDiagnosticBundleClick: (s: string) => void) => {
        return (_text: string, record: IStatementDiagnosticsReport) => {
          if (record.completed) {
            return (
              <div
                className={cx(
                  "crl-statements-diagnostics-view__actions-column",
                )}
              >
                <a
                  href={`${getBasePath()}/_admin/v1/stmtbundle/${
                    record.statement_diagnostics_id
                  }`}
                  onClick={() =>
                    onDownloadDiagnosticBundleClick &&
                    onDownloadDiagnosticBundleClick(
                      record.statement_fingerprint,
                    )
                  }
                >
                  <Button
                    size="small"
                    type="flat"
                    iconPosition="left"
                    icon={<Download />}
                  >
                    Bundle (.zip)
                  </Button>
                </a>
              </div>
            );
          }
          return null;
        };
      })(this.props.onDownloadDiagnosticBundleClick),
    },
  ];

  onActivateButtonClick = () => {
    const { activate, statementFingerprint } = this.props;
    activate(statementFingerprint);
  };

  componentWillUnmount() {
    this.props.dismissAlertMessage();
  }

  onSortingChange = (columnName: string, ascending: boolean) => {
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Diagnostics", columnName, ascending);
    }
  };

  render() {
    const { hasData, diagnosticsReports, showDiagnosticsViewLink } = this.props;

    const canRequestDiagnostics = diagnosticsReports.every(
      diagnostic => diagnostic.completed,
    );

    const dataSource = diagnosticsReports.map((diagnosticsReport, idx) => ({
      ...diagnosticsReport,
      key: idx,
    }));

    if (!hasData) {
      return (
        <SummaryCard className={cx("summary--card__empty-state")}>
          <EmptyDiagnosticsView {...this.props} />
        </SummaryCard>
      );
    }

    return (
      <SummaryCard>
        <div className={cx("crl-statements-diagnostics-view__title")}>
          <Text textType={TextTypes.Heading3}>Statement diagnostics</Text>
          {canRequestDiagnostics && (
            <Button
              onClick={this.onActivateButtonClick}
              disabled={!canRequestDiagnostics}
              type="secondary"
              className={cx("crl-statements-diagnostics-view__activate-button")}
            >
              Activate
            </Button>
          )}
        </div>
        <Table
          dataSource={dataSource}
          columns={this.columns}
          onSortingChange={this.onSortingChange}
        />
        {showDiagnosticsViewLink && (
          <div className={cx("crl-statements-diagnostics-view__footer")}>
            <Link
              to="/reports/statements/diagnosticshistory"
              className={cx("anchor")}
            >
              All statement diagnostics
            </Link>
          </div>
        )}
      </SummaryCard>
    );
  }
}
