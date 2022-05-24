// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { JobDetailsProps } from "src/views/jobs/jobDetails";
import { cockroach } from "src/js/protos";
import Job = cockroach.server.serverpb.JobResponse;
type Timestamp = protos.google.protobuf.ITimestamp;
import { createMemoryHistory } from "history";
import Long from "long";
import { defaultJobProperties } from "src/views/jobs/jobsShared.fixture";
import * as protos from "src/js/protos";

const history = createMemoryHistory({ initialEntries: ["/statements"] });

export const getJobDetailsProps = (job: Job): JobDetailsProps => {
  return {
    history,
    location: {
      pathname: "/jobs/760621514586259457",
      search: "",
      hash: "",
      state: undefined,
    },
    match: {
      path: "/jobs/:id",
      url: "/jobs/760621514586259457",
      isExact: true,
      params: { id: "/jobs/760621514586259457" },
    },
    sort: {
      columnTitle: "startTime",
      ascending: false,
    },
    setSort: () => {},
    refreshJob: () => null,
    job: job,
  };
};

export const succeededJobProps = getJobDetailsProps(
  new Job({
    ...defaultJobProperties,
    id: new Long(7003330561, 70312826),
    type: "SCHEMA CHANGE",
    description:
      "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
    status: "succeeded",
  }),
);

export const failedJobProps = getJobDetailsProps(
  new Job({
    ...defaultJobProperties,
    id: new Long(7003330561, 70312826),
    type: "SCHEMA CHANGE",
    description:
      "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
    status: "failed",
    error:
      "Mock very very very very very very very very very very very very very very very very very very very very very very very very very very very very very long failure message",
    execution_failures: [], // no retriable errors
  }),
);

const getDetaultStartPlusTime = (
  seconds: number = 0,
  nanos: number = 0,
): Timestamp => {
  return {
    seconds: defaultJobProperties.started.seconds.add(seconds),
    nanos: defaultJobProperties.started.nanos + nanos,
  };
};

export const hypotheticalRunningWithRetriableErrorsJobProps = getJobDetailsProps(
  new Job({
    ...defaultJobProperties,
    id: new Long(7003330561, 70312826),
    type: "SCHEMA CHANGE",
    description:
      "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
    status: "retrying",
    error: "", // no final error
    execution_failures: [
      {
        start: getDetaultStartPlusTime(60 * 2),
        end: getDetaultStartPlusTime(60 * 4),
        status: "running",
        error: "First retriable error.",
      },
      {
        start: getDetaultStartPlusTime(60 * 6),
        end: getDetaultStartPlusTime(60 * 8),
        status: "running",
        error: "Second mock retriable error.",
      },
    ],
  }),
);

export const hypotheticalFailedWithRetriableErrorsJobProps = getJobDetailsProps(
  new Job({
    ...defaultJobProperties,
    id: new Long(7003330561, 70312826),
    type: "SCHEMA CHANGE",
    description:
      "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
    status: "failed",
    error: "Mock final failure error message",
    execution_failures: [
      {
        start: getDetaultStartPlusTime(60 * 2),
        end: getDetaultStartPlusTime(60 * 4),
        status: "running",
        error: "First retriable error.",
      },
      {
        start: getDetaultStartPlusTime(60 * 6),
        end: getDetaultStartPlusTime(60 * 8),
        status: "running",
        error: "Second mock retriable error.",
      },
    ],
  }),
);
