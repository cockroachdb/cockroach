// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React, { useState } from "react";
import { storiesOf } from "@storybook/react";
import { withRouterDecorator } from "src/util/decorators";

import {
  succeededJobProps,
  failedJobProps,
  hypotheticalRunningWithRetriableErrorsJobProps,
  hypotheticalFailedWithRetriableErrorsJobProps,
} from "./jobDetails.fixture";
import {
  JobDetails,
  JobDetailsProps,
  defaultSortSetting,
} from "src/views/jobs/jobDetails";
import { SortSetting } from "@cockroachlabs/cluster-ui";

export function JobDetailsSortWrapper(
  props: JobDetailsProps,
): React.ReactElement {
  const [sortSetting, setSortSetting] = useState<SortSetting>(
    defaultSortSetting,
  );
  return <JobDetails {...props} sort={sortSetting} setSort={setSortSetting} />;
}

storiesOf("JobDetails", module)
  .addDecorator(withRouterDecorator)
  .add("Succeed", () => <JobDetailsSortWrapper {...succeededJobProps} />)
  .add("Failed", () => <JobDetailsSortWrapper {...failedJobProps} />)
  .add("Running with retriable errors", () => (
    <JobDetailsSortWrapper
      {...hypotheticalRunningWithRetriableErrorsJobProps}
    />
  ))
  .add("Failed with retriable errors", () => (
    <JobDetailsSortWrapper {...hypotheticalFailedWithRetriableErrorsJobProps} />
  ));
