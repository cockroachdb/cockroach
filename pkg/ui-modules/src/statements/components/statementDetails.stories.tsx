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
import { storiesOf } from "@storybook/react";
import { Location } from "history";

import {withBackgroundFactory, withRouterProvider} from ".storybook/decorators";
import {StatementDetails} from "./statementDetails";
import {statementDetailsPropsFixture} from "./statementDetails.fixture";

// (koorosh) Note: Diagnostics tab isn't added here because it is independent
// connected view and has to be managed as separate story.

storiesOf("StatementDetails", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackgroundFactory())
  .add("Overview tab", () => (
    <StatementDetails
      {...statementDetailsPropsFixture}
    />
  ))
  .add("Logical Plan tab", () => {
    const location: Location = {
      ...statementDetailsPropsFixture.history.location,
      search: new URLSearchParams([["tab", "logical-plan"]]).toString(),
    };
    return (<StatementDetails
      {...statementDetailsPropsFixture}
      history={{
        ...statementDetailsPropsFixture.history,
        location,
      }}
    />);
  })
  .add("Execution Stats tab", () => {
    const location: Location = {
      ...statementDetailsPropsFixture.history.location,
      search: new URLSearchParams([["tab", "execution-stats"]]).toString(),
    };
    return (<StatementDetails
      {...statementDetailsPropsFixture}
      history={{
        ...statementDetailsPropsFixture.history,
        location,
      }}
    />);
  });
