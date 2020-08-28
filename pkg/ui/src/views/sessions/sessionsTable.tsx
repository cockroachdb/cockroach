// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";
import {ColumnDescriptor, SortedTable} from "src/views/shared/components/sortedtable";
import "./sessions.styl";
import {cockroach} from "src/js/protos";
import styles from "./sessionsTable.module.styl";
import {SessionTableTitle} from "src/views/sessions/sessionsTableContent";
import {TimestampToMoment} from "src/util/convert";
import {BytesWithPrecision, DATE_FORMAT} from "src/util/format";
import {Link} from "react-router-dom";
import React, {useState} from "react";
import {Anchor, Tooltip} from "src/components";
import {Moment} from "moment";
import {StatementLink} from "src/views/statements/statementsTableContent";
import ISession = cockroach.server.serverpb.ISession;
import {Button, Menu, Popover} from "antd";
import {TerminateSessionModalRef} from "src/views/sessions/terminateSessionModal";
import {TerminateQueryModalRef} from "src/views/sessions/terminateQueryModal";

const cx = classNames.bind(styles);

export interface SessionInfo {
  session: ISession;
}

export class SessionsSortedTable extends SortedTable<SessionInfo> {}

export function makeSessionsColumns(
  terminateSessionRef?: React.RefObject<TerminateSessionModalRef>,
  terminateQueryRef?: React.RefObject<TerminateQueryModalRef>,
): ColumnDescriptor<SessionInfo>[]  {
  const [openState, setIsOpen] = useState<string>("");
  return [
    {
      title: SessionTableTitle.sessionAge,
      className: cx("cl-table__col-session-age"),
      cell: session => SessionLink({session: session.session}),
      sort: session => TimestampToMoment(session.session.start).valueOf(),
    },
    {
      title: SessionTableTitle.txnAge,
      className: cx("cl-table__col-session-start"),
      cell: function(session: SessionInfo) {
        if (session.session.active_txn) {
          return AgeLabel({
            start: TimestampToMoment(session.session.active_txn.start),
            thingName: "Transaction",
          });
        }
        return "N/A";
      },
      sort: session => session.session.active_txn?.start.seconds || 0,
    },
    {
      title: SessionTableTitle.statementAge,
      className: cx("cl-table__col-session-start"),
      cell: function(session: SessionInfo) {
        if (session.session.active_queries?.length > 0) {
          return AgeLabel({
            start: TimestampToMoment(session.session.active_queries[0].start),
            thingName: "Query",
          });
        }
        return "N/A";
      },
      sort: function(session: SessionInfo): number {
        if (session.session.active_queries?.length > 0) {
          return session.session.active_queries[0].start.seconds.toNumber();
        }
        return 0;
      },
    },
    {
      title: SessionTableTitle.memUsage,
      className: cx("cl-table__col-session-mem-usage"),
      cell: session => BytesWithPrecision(session.session.alloc_bytes?.toNumber(), 0) +
        "/" + BytesWithPrecision(session.session.max_alloc_bytes?.toNumber(), 0),
      sort: session => session.session.alloc_bytes?.toNumber(),
    },
    {
      title: SessionTableTitle.statement,
      className: cx("cl-table__col-query-text"),
      cell: (session) => {
        if (!(session.session.active_queries?.length > 0)) {
          return "N/A";
        }
        const stmt = session.session.active_queries[0].sql;
        const anonStmt = session.session.active_queries[0].sql_anon;
        return <StatementLink
          statement={ stmt }
          anonStatement={ anonStmt }
          implicitTxn={ session.session.active_txn?.implicit }
          search={ "" }
          app={ "" }
        />;
      },
    },
    {
      title: SessionTableTitle.actions,
      className: cx("cl-table__col-session-actions"),
      cell: ({session}) => {
        const sessionId = session.id.join("");
        const menuItems = (
          <Menu>
            <Menu.Item
              disabled={session.active_queries?.length === 0}
            >
              <Anchor
                onClick={() => {
                  if (session.active_queries?.length > 0) {
                    terminateQueryRef?.current?.showModalFor({
                      query_id: session.active_queries[0].id,
                      node_id: session.node_id.toString(),
                    });
                  }
                }}
              >
                Terminate Query
              </Anchor>
            </Menu.Item>
            <Menu.Item>
              <Anchor
                onClick={() => terminateSessionRef?.current?.showModalFor({
                  session_id: session.id,
                  node_id: session.node_id.toString(),
                })}
              >
                Terminate Session
              </Anchor>
            </Menu.Item>
          </Menu>
        );
        return (
          <Popover
            placement="bottomRight"
            trigger="click"
            visible={openState === sessionId}
            onVisibleChange={(isVisible: boolean) => {
              if (!isVisible) {
                setIsOpen("");
              } else {
                setIsOpen(sessionId);
              }
            }}
            content={menuItems}
          >
            <Button>
              ...
            </Button>
          </Popover>
        );
      },
    },
  ];
}

export function byteArrayToUuid(array: Uint8Array) {
  const hexDigits: string[] = [];
  array.forEach(t => hexDigits.push(t.toString(16).padStart(2, "0")));
  return [hexDigits.slice(0, 4).join(""),
      hexDigits.slice(4, 6).join(""),
      hexDigits.slice(6, 8).join(""),
      hexDigits.slice(8, 10).join(""),
      hexDigits.slice(10, 16).join("")].join("-");
}

const SessionLink = (props: { session: ISession }) => {
  const base = `/session`;
  const start = TimestampToMoment(props.session.start);
  const sessionID = byteArrayToUuid(props.session.id);

  return (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            Session started at {start.format(DATE_FORMAT)}
          </p>
        </div>
      }
    >
      <Link to={ `${base}/${encodeURIComponent(sessionID)}` }>
        <div>
          { start.fromNow(true) }
        </div>
      </Link>
    </Tooltip>
  );
};

const AgeLabel = (props: { start: Moment, thingName: string }) => {
  return (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {props.thingName} started at {props.start.format(DATE_FORMAT)}
          </p>
        </div>
      }
    >
    { props.start.fromNow(true) }
    </Tooltip>
  );
};
