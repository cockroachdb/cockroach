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

import { Button, Text, TextTypes, Table, ColumnsConfig, Badge, Link } from "src/components";
import { AdminUIState } from "src/redux/state";
import { SummaryCard } from "src/views/shared/components/summaryCard";
import { selectStatementById } from "src/redux/statements/statementsSelectors";
import { enqueueDiagnostics } from "src/redux/statements";
import { trustIcon } from "src/util/trust";

import DownloadIcon from "!!raw-loader!assets/download.svg";
import "./diagnosticsView.styl";

interface DiagnosticsViewOwnProps {
  statementId: string;
}

type DiagnosticsViewProps = DiagnosticsViewOwnProps & MapStateToProps & MapDispatchToProps;

function mapDiagnosticsStatusToBadge(diagnosticsStatus: string) {
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
    const { hasData, statement } = this.props;
    const diagnostics: any[] = statement.diagnostics || [];

    const canRequestDiagnostics = diagnostics.every(diagnostic => diagnostic.status !== "WAITING FOR QUERY");

    const dataSource = diagnostics.map((diagnostic: any, idx: number) => ({
      key: idx,
      activatedOn: diagnostic.initiated_at,
      status: diagnostic.status,
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
  activateDiagnostics = () => {
    const { statementId, activate } = this.props;
    activate(statementId);
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
              table statistics, and traces. <Link href="https://www.cockroachlabs.com/docs/stable">Learn more</Link>
            </Text>
          {/*  TODO (koorosh): change Learn more link to something meaningful ^^^. */}
          </main>
          <footer className="crl-statements-diagnostics-view__footer">
            <Button
              type="primary"
              onClick={this.activateDiagnostics}
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
  statement: any;
}

interface MapDispatchToProps {
  activate: (statementId: string) => void;
}

const mapStateToProps = (state: AdminUIState, props: DiagnosticsViewProps): MapStateToProps => {
  const statement = selectStatementById(state, props.statementId);
  const hasData = statement.diagnostics.length > 0;
  return {
    hasData,
    statement,
  };
};

const mapDispatchToProps: MapDispatchToProps = {
  activate: enqueueDiagnostics,
};

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  DiagnosticsViewOwnProps
  >(mapStateToProps, mapDispatchToProps)(DiagnosticsView);
