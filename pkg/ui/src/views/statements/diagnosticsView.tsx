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
import { connect } from "react-redux";
import { Link as RouterLink } from "react-router-dom";
import moment from "moment";

import { Button, Text, TextTypes, Table, ColumnsConfig, Badge, Anchor } from "src/components";
import { AdminUIState } from "src/redux/state";
import { SummaryCard } from "src/views/shared/components/summaryCard";
import {
  selectDiagnosticsReportsByStatementFingerprint,
  selectDiagnosticsReportsCountByStatementFingerprint,
} from "src/redux/statements/statementsSelectors";
import { requestStatementDiagnosticsReport } from "src/redux/statements";
import { trustIcon } from "src/util/trust";
import DownloadIcon from "!!raw-loader!assets/download.svg";
import "./diagnosticsView.styl";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

interface DiagnosticsViewOwnProps {
  statementFingerprint?: string;
}

type DiagnosticsViewProps = DiagnosticsViewOwnProps & MapStateToProps & MapDispatchToProps;

type DiagnosticsStatuses = "READY" | "WAITING FOR QUERY" | "ERROR";

function mapDiagnosticsStatusToBadge(diagnosticsStatus: DiagnosticsStatuses) {
  switch (diagnosticsStatus) {
    case "READY":
      return "success";
    case "WAITING FOR QUERY":
      return "info";
    case "ERROR":
      return "danger";
    default:
      return "info";
  }
}

function getDiagnosticsStatus(diagnosticsRequest: IStatementDiagnosticsReport): DiagnosticsStatuses {
  if (diagnosticsRequest.completed) {
    return "READY";
  }

  return "WAITING FOR QUERY";
}

export class DiagnosticsView extends React.Component<DiagnosticsViewProps> {
  columns: ColumnsConfig<any> = [
    {
      key: "activatedOn",
      title: "Activated on",
      sorter: true,
      render: (_text, record) => {
        return moment(record.activatedOn).format("LL[ at ]h:mm a");
      },
    },
    {
      key: "status",
      title: "status",
      sorter: true,
      width: "60px",
      render: (_text, record) => (
        <Badge
          text={record.status}
          status={mapDiagnosticsStatusToBadge(record.status)}
        />
      ),
    },
    {
      key: "actions",
      title: "",
      sorter: false,
      width: "160px",
      render: (_text, record) => {
        if (record.status === "READY") {
          return (
            <div className="crl-statements-diagnostics-view__actions-column">
              <Button
                size="small"
                type="flat"
                iconPosition="left"
                icon={() => (
                  <span
                    className="crl-statements-diagnostics-view__icon"
                    dangerouslySetInnerHTML={ trustIcon(DownloadIcon) }
                  />
                )}
              >
                Download
              </Button>
              <div className="crl-statements-diagnostics-view__vertical-line" />
              <Button size="small" type="flat">View trace</Button>
            </div>
          );
        }
        return null;
      },
    },
  ];

  render() {
    const { hasData, diagnosticsReports, activate, statementFingerprint } = this.props;

    const canRequestDiagnostics = diagnosticsReports.every(diagnostic => diagnostic.completed);

    const dataSource = diagnosticsReports.map((diagnostic, idx) => ({
      key: idx,
      activatedOn: diagnostic.requested_at.seconds.toNumber() * 1000,
      status: getDiagnosticsStatus(diagnostic),
    }));

    if (!hasData) {
      return (
        <SummaryCard className="summary--card__empty-sate">
          <EmptyDiagnosticsView {...this.props} />
        </SummaryCard>
      );
    }
    return (
      <SummaryCard>
        <div
          className="crl-statements-diagnostics-view__title"
        >
          <Text
            textType={TextTypes.Heading3}
          >
            Statement diagnostics
          </Text>
          <Button
            onClick={() => activate(statementFingerprint)}
            disabled={!canRequestDiagnostics}
            type="secondary"
          >
            Activate diagnostics
          </Button>
        </div>
        <Table
          dataSource={dataSource}
          columns={this.columns}
        />
        <div className="crl-statements-diagnostics-view__footer">
          <RouterLink to="/reports/statements/diagnostics">All statement diagnostics</RouterLink>
        </div>
      </SummaryCard>
    );
  }
}

export class EmptyDiagnosticsView extends React.Component<DiagnosticsViewProps> {

  onActivateButtonClick = () => {
    const { activate, statementFingerprint } = this.props;
    activate(statementFingerprint);
  }

  render() {
    return (
      <div className="crl-statements-diagnostics-view">
        <Text
          className="crl-statements-diagnostics-view__title"
          textType={TextTypes.Heading3}
        >
          Activate statement diagnostics
        </Text>
        <div className="crl-statements-diagnostics-view__content">
          <main className="crl-statements-diagnostics-view__main">
            <Text
              textType={TextTypes.Body}
            >
              When you activate statement diagnostics, CockroachDB will wait for the next query that matches
              this statement fingerprint. A download button will appear on the statement list and detail pages
              when the query is ready. The statement diagnostic will include EXPLAIN plans,
              table statistics, and traces. <Anchor href="https://www.cockroachlabs.com/docs/stable">Learn more</Anchor>
            </Text>
          {/*  TODO (koorosh): change Learn more link to something meaningful ^^^. */}
          </main>
          <footer className="crl-statements-diagnostics-view__footer">
            <Button
              type="primary"
              onClick={this.onActivateButtonClick}
            >
              Activate
            </Button>
          </footer>
        </div>
      </div>
    );
  }
}

interface MapStateToProps {
  hasData: boolean;
  diagnosticsReports: IStatementDiagnosticsReport[];
}

interface MapDispatchToProps {
  activate: (statementFingerprint: string) => void;
}

const mapStateToProps = (state: AdminUIState, props: DiagnosticsViewProps): MapStateToProps => {
  const { statementFingerprint } = props;
  const hasData = selectDiagnosticsReportsCountByStatementFingerprint(state, statementFingerprint) > 0;
  const diagnosticsReports = selectDiagnosticsReportsByStatementFingerprint(state, statementFingerprint);
  return {
    hasData,
    diagnosticsReports,
  };
};

const mapDispatchToProps: MapDispatchToProps = {
  activate: requestStatementDiagnosticsReport,
};

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  DiagnosticsViewOwnProps
  >(mapStateToProps, mapDispatchToProps)(DiagnosticsView);
