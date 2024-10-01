// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Icon, Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React from "react";
import { Link } from "react-router-dom";

import { Button } from "src/button/button";
import {
  Dropdown,
  DropdownOption as DropdownItem,
} from "src/dropdown/dropdown";
import { CircleFilled } from "src/icon/circleFilled";
import { ColumnDescriptor, SortedTable } from "src/sortedtable/sortedtable";
import {
  DurationToMomentDuration,
  DurationToNumber,
  TimestampToMoment,
} from "src/util/convert";
import { BytesWithPrecision, Count, DATE_FORMAT } from "src/util/format";

import {
  statisticsTableTitles,
  StatisticType,
} from "../statsTableUtil/statsTableUtil";
import { Timestamp } from "../timestamp";
import { computeOrUseStmtSummary, FixLong } from "../util";

import styles from "./sessionsTable.module.scss";
import { TerminateQueryModalRef } from "./terminateQueryModal";
import { TerminateSessionModalRef } from "./terminateSessionModal";

type ISession = cockroach.server.serverpb.ISession;
type Status = cockroach.server.serverpb.Session.Status;

const cx = classNames.bind(styles);

export interface SessionInfo {
  session: ISession;
}

export class SessionsSortedTable extends SortedTable<SessionInfo> {}

export function byteArrayToUuid(array: Uint8Array): string {
  const hexDigits: string[] = [];
  array.forEach(t => hexDigits.push(t.toString(16).padStart(2, "0")));
  return [
    hexDigits.slice(0, 4).join(""),
    hexDigits.slice(4, 6).join(""),
    hexDigits.slice(6, 8).join(""),
    hexDigits.slice(8, 10).join(""),
    hexDigits.slice(10, 16).join(""),
  ].join("-");
}

const SessionLink = (props: { session: ISession; onClick?: () => void }) => {
  const { session, onClick } = props;

  const base = `/session`;
  const sessionID = byteArrayToUuid(session.id);

  return (
    <div className={cx("sessionLink")}>
      <Link onClick={onClick} to={`${base}/${encodeURIComponent(sessionID)}`}>
        <div>{formatSessionStart(session)}</div>
      </Link>
    </div>
  );
};

const StatementTableCell = (props: { session: ISession }) => {
  const { session } = props;

  if (!(session.active_queries?.length > 0)) {
    if (session.last_active_query === "") {
      return <div>{"N/A"}</div>;
    }
    return <div>{session.last_active_query}</div>;
  }
  const stmt = session.active_queries[0];
  const sql = stmt.sql;
  const sqlNoConstants = stmt.sql_no_constants;
  const stmtQuery = sql.length > 0 ? sql : sqlNoConstants;
  const stmtSummary = stmt.sql_summary;
  const stmtCellText = computeOrUseStmtSummary(stmtQuery, stmtSummary);
  return (
    <Link to={`execution/statement/${stmt.id}`}>
      <Tooltip placement="bottom" style="tableTitle" content={<>{sql}</>}>
        <div className={cx("cl-table__col-query-text")}>{stmtCellText}</div>
      </Tooltip>
    </Link>
  );
};

function formatSessionStart(session: ISession) {
  const start = moment.unix(Number(session.start.seconds)).utc();
  return <Timestamp time={start} format={DATE_FORMAT} />;
}

function formatStatementStart(session: ISession) {
  if (session.active_queries.length === 0) {
    return <>N/A</>;
  }
  const start = moment
    .unix(Number(session.active_queries[0].start.seconds))
    .utc();

  return <Timestamp time={start} format={DATE_FORMAT} />;
}

export function getStatusString(status: Status): string {
  switch (status) {
    case 0:
      return "Active";
    case 1:
      return "Closed";
    case 2:
      return "Idle";
  }
}

export function getStatusClassname(status: Status): string {
  switch (status) {
    case 0:
      return "session-status-icon__active";
    case 1:
      return "session-status-icon__closed";
    case 2:
      return "session-status-icon__idle";
  }
}

const SessionStatus = (props: { session: ISession }) => {
  const { session } = props;
  const status = getStatusString(session.status);
  const classname = getStatusClassname(session.status);
  return (
    <div>
      <CircleFilled className={cx(classname)} />
      <span>{status}</span>
    </div>
  );
};

export function makeSessionsColumns(
  statType: StatisticType,
  terminateSessionRef?: React.RefObject<TerminateSessionModalRef>,
  terminateQueryRef?: React.RefObject<TerminateQueryModalRef>,
  onSessionClick?: () => void,
  onTerminateSessionClick?: () => void,
  onTerminateStatementClick?: () => void,
): ColumnDescriptor<SessionInfo>[] {
  const columns: ColumnDescriptor<SessionInfo>[] = [
    {
      name: "sessionStart",
      title: statisticsTableTitles.sessionStart(statType),
      className: cx("cl-table__col-session"),
      cell: session =>
        SessionLink({ session: session.session, onClick: onSessionClick }),
      sort: session => session.session.start.seconds,
      alwaysShow: true,
    },
    {
      name: "sessionDuration",
      title: statisticsTableTitles.sessionDuration(statType),
      className: cx("cl-table__col-session"),
      cell: session => {
        const startMoment = TimestampToMoment(session.session.start);
        if (session.session.end != null) {
          return TimestampToMoment(session.session.end).from(startMoment, true);
        }
        return startMoment.fromNow(true);
      },
      sort: session => TimestampToMoment(session.session.start).valueOf(),
    },
    {
      name: "sessionActiveDuration",
      title: statisticsTableTitles.sessionActiveDuration(statType),
      className: cx("cl-table__col-session"),
      cell: session =>
        DurationToMomentDuration(session.session.total_active_time).humanize(),
      sort: session => DurationToNumber(session.session.total_active_time),
    },
    {
      name: "status",
      title: statisticsTableTitles.status(statType),
      className: cx("cl-table__col-session"),
      cell: session => SessionStatus(session),
      sort: session => session.session.active_queries.length,
    },
    {
      name: "mostRecentStatement",
      title: statisticsTableTitles.mostRecentStatement(statType),
      className: cx("cl-table__col-query-text"),
      cell: session => StatementTableCell(session),
      sort: session => session.session.last_active_query,
    },
    {
      name: "statementStartTime",
      title: statisticsTableTitles.statementStartTime(statType),
      className: cx("cl-table__col-session"),
      cell: session => formatStatementStart(session.session),
      sort: session =>
        session.session.active_queries.length > 0
          ? session.session.active_queries[0].start.seconds
          : 0,
    },
    {
      name: "sessionTxnCount",
      title: statisticsTableTitles.sessionTxnCount(statType),
      className: cx("cl-table__col-session"),
      cell: session => Count(session.session?.num_txns_executed),
      sort: session => session.session?.num_txns_executed,
    },
    {
      name: "memUsage",
      title: statisticsTableTitles.memUsage(statType),
      className: cx("cl-table__col-session"),
      cell: session =>
        BytesWithPrecision(
          FixLong(session.session.alloc_bytes ?? 0).toNumber(),
          0,
        ) +
        "/" +
        BytesWithPrecision(
          FixLong(session.session.max_alloc_bytes ?? 0).toNumber(),
          0,
        ),
      sort: session => FixLong(session.session.alloc_bytes ?? 0).toNumber(),
    },
    {
      name: "clientAddress",
      title: statisticsTableTitles.clientAddress(statType),
      className: cx("cl-table__col-session"),
      cell: session => session.session.client_address,
      sort: session => session.session.client_address,
    },
    {
      name: "username",
      title: statisticsTableTitles.username(statType),
      className: cx("cl-table__col-session"),
      cell: session => session.session.username,
      sort: session => session.session.username,
    },
    {
      name: "applicationName",
      title: statisticsTableTitles.applicationName(statType),
      className: cx("cl-table__col-session"),
      cell: session => session.session.application_name,
      sort: session => session.session.application_name,
    },
    {
      name: "actions",
      title: statisticsTableTitles.actions(statType),
      className: cx("cl-table__col-session-actions"),
      titleAlign: "right",
      cell: ({ session }) => {
        const menuItems: DropdownItem<"cancelStatement" | "cancelSession">[] = [
          {
            value: "cancelStatement",
            name: "Cancel Statement",
            disabled: session.active_queries?.length === 0,
          },
          {
            value: "cancelSession",
            name: "Cancel Session",
          },
        ];

        const onMenuItemChange = (
          value: "cancelStatement" | "cancelSession",
        ) => {
          switch (value) {
            case "cancelSession":
              onTerminateSessionClick && onTerminateSessionClick();
              terminateSessionRef?.current?.showModalFor({
                session_id: session.id,
                node_id: session.node_id.toString(),
              });
              break;
            case "cancelStatement":
              if (session.active_queries?.length > 0) {
                onTerminateStatementClick && onTerminateStatementClick();
                terminateQueryRef?.current?.showModalFor({
                  query_id: session.active_queries[0].id,
                  node_id: session.node_id.toString(),
                });
              }
              break;
            default:
              break;
          }
        };

        const renderDropdownToggleButton: JSX.Element = (
          <>
            <Button type="secondary" size="small">
              <Icon
                iconName="Ellipsis"
                className="cluster-row__status-icon"
                color="warning-color"
              />
            </Button>
          </>
        );

        return (
          <Dropdown
            items={menuItems}
            customToggleButton={renderDropdownToggleButton}
            onChange={onMenuItemChange}
            className={cx("session-action--dropdown")}
            menuPosition="right"
          />
        );
      },
      alwaysShow: true,
    },
  ];

  return columns;
}
