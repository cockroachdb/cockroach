// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createMemoryHistory } from "history";
import Long from "long";
import { util } from "protobufjs";

import {
  CancelQueryRequestMessage,
  CancelSessionRequestMessage,
} from "src/api/terminateQueryApi";

import { defaultFilters, Filters } from "../queryFilter";

import { SessionsPageProps } from "./sessionsPage";
import { SessionInfo } from "./sessionsTable";

const Status = cockroach.server.serverpb.Session.Status;
const Phase = cockroach.server.serverpb.ActiveQuery.Phase;

const history = createMemoryHistory({ initialEntries: ["/sessions"] });

const toUuid = function (s: string): Uint8Array {
  const buf = util.newBuffer(util.base64.length(s));
  util.base64.decode(s, buf, 0);
  return buf;
};

export const idleSession: SessionInfo = {
  session: {
    node_id: 1,
    username: "root",
    client_address: "127.0.0.1:57618",
    application_name: "$ cockroach sql",
    start: {
      seconds: Long.fromNumber(1596816670),
      nanos: 369989000,
    },
    last_active_query: "SHOW database",
    id: toUuid("FikITSjUZoAAAAAAAAAAAQ=="),
    last_active_query_no_constants: "SHOW database",
    alloc_bytes: Long.fromNumber(0),
    max_alloc_bytes: Long.fromNumber(10240),
    active_queries: [],
    num_txns_executed: 1,
    txn_fingerprint_ids: [],
    status: Status.IDLE,
    pg_backend_pid: 123,
    trace_id: Long.fromNumber(123),
    goroutine_id: Long.fromNumber(456),
  },
};

export const idleTransactionSession: SessionInfo = {
  session: {
    node_id: 1,
    username: "root",
    client_address: "127.0.0.1:57623",
    application_name: "$ cockroach sql",
    alloc_bytes: Long.fromNumber(0),
    max_alloc_bytes: Long.fromNumber(10240),
    start: {
      seconds: Long.fromNumber(1596816671),
      nanos: 835905000,
    },
    last_active_query: "SHOW database",
    id: toUuid("FikITYA0lGgAAAAAAAAAAQ=="),
    active_txn: {
      id: toUuid("LDzmvKMqTvaVhIaBhejfgw=="),
      start: {
        seconds: Long.fromNumber(1596816673),
        nanos: 134293000,
      },
      txn_description:
        '"sql txn" meta={id=2c3ce6bc key=/Min pri=0.04688813 epo=0 ts=1596816673.134285000,0 min=1596816673.134285000,0 seq=0} lock=false stat=PENDING rts=1596816673.134285000,0 wto=false max=1596816673.634285000,0',
      num_statements_executed: 2,
      deadline: {
        seconds: Long.fromNumber(-62135596800),
      },
      priority: "normal",
      implicit: false,
      num_retries: 5,
      num_auto_retries: 3,
    },
    last_active_query_no_constants: "SHOW database",
    active_queries: [],
    num_txns_executed: 1,
    txn_fingerprint_ids: [],
    status: Status.IDLE,
    pg_backend_pid: 123,
    trace_id: Long.fromNumber(123),
    goroutine_id: Long.fromNumber(456),
  },
};

export const activeSession: SessionInfo = {
  session: {
    node_id: 1,
    username: "root",
    client_address: "127.0.0.1:57632",
    application_name: "$ cockroach sql",
    active_queries: [
      {
        id: "16290b41dddca4600000000000000001",
        sql: "SELECT pg_sleep(1000)",
        start: {
          seconds: Long.fromNumber(1596819920),
          nanos: 402524000,
        },
        phase: Phase.EXECUTING,
        txn_id: toUuid("e8NTvpOvScO1tSMreygtcg=="),
        sql_no_constants: "SELECT pg_sleep(_)",
      },
    ],
    start: {
      seconds: Long.fromNumber(1596816675),
      nanos: 652814000,
    },
    last_active_query: "SHOW database",
    id: toUuid("FikITmO2BQAAAAAAAAAAAQ=="),
    alloc_bytes: Long.fromNumber(10240),
    max_alloc_bytes: Long.fromNumber(10240),
    active_txn: {
      id: toUuid("e8NTvpOvScO1tSMreygtcg=="),
      start: {
        seconds: Long.fromNumber(1596816677),
        nanos: 320351000,
      },
      txn_description:
        '"sql txn" meta={id=7bc353be key=/Min pri=0.05293838 epo=0 ts=1596816677.320344000,0 min=1596816677.320344000,0 seq=0} lock=false stat=PENDING rts=1596816677.320344000,0 wto=false max=1596816677.820344000,0',
      num_statements_executed: 4,
      deadline: {
        seconds: Long.fromNumber(-62135596800),
      },
      alloc_bytes: Long.fromNumber(0),
      max_alloc_bytes: Long.fromNumber(10240),
      priority: "normal",
      implicit: true,
      num_retries: 5,
      num_auto_retries: 3,
    },
    last_active_query_no_constants: "SHOW database",
    status: Status.ACTIVE,
    num_txns_executed: 1,
    txn_fingerprint_ids: [],
    pg_backend_pid: 123,
    trace_id: Long.fromNumber(123),
    goroutine_id: Long.fromNumber(456),
  },
};

export const closedSession: SessionInfo = {
  session: {
    node_id: 1,
    username: "root",
    client_address: "127.0.0.1:57618",
    application_name: "$ cockroach sql",
    start: {
      seconds: Long.fromNumber(1596816670),
      nanos: 369989000,
    },
    last_active_query: "SHOW database",
    id: toUuid("FekiTsjUZoAAAAAAAAAAAQ=="),
    last_active_query_no_constants: "SHOW database",
    alloc_bytes: Long.fromNumber(0),
    max_alloc_bytes: Long.fromNumber(10240),
    active_queries: [],
    end: {
      seconds: Long.fromNumber(1596819870),
      nanos: 369989000,
    },
    status: Status.CLOSED,
    num_txns_executed: 1,
    txn_fingerprint_ids: [],
    pg_backend_pid: 123,
    trace_id: Long.fromNumber(123),
    goroutine_id: Long.fromNumber(456),
  },
};

const sessionsList: SessionInfo[] = [
  idleSession,
  idleTransactionSession,
  activeSession,
  closedSession,
];

export const filters: Filters = {
  app: "",
  timeNumber: "0",
  timeUnit: "seconds",
  regions: "",
  nodes: "",
};

export const sessionsPagePropsFixture: SessionsPageProps = {
  filters: defaultFilters,
  history,
  location: {
    pathname: "/sessions",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/sessions",
    url: "/sessions",
    isExact: true,
    params: {},
  },
  sessions: sessionsList,
  sessionsError: null,
  sortSetting: {
    ascending: false,
    columnTitle: "statementAge",
  },
  columns: null,
  internalAppNamePrefix: "$ internal",
  refreshSessions: () => {},
  cancelSession: (_req: CancelSessionRequestMessage) => {},
  cancelQuery: (_req: CancelQueryRequestMessage) => {},
  onSortingChange: () => {},
};

export const sessionsPagePropsEmptyFixture: SessionsPageProps = {
  filters: defaultFilters,
  history,
  location: {
    pathname: "/sessions",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/sessions",
    url: "/sessions",
    isExact: true,
    params: {},
  },
  sessions: [],
  sessionsError: null,
  sortSetting: {
    ascending: false,
    columnTitle: "statementAge",
  },
  columns: null,
  internalAppNamePrefix: "$ internal",
  refreshSessions: () => {},
  cancelSession: (_req: CancelSessionRequestMessage) => {},
  cancelQuery: (_req: CancelQueryRequestMessage) => {},
  onSortingChange: () => {},
};
