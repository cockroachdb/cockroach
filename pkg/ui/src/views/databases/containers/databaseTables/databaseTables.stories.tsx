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
import { DatabaseSummaryTables } from "./";
import {
  dbEmptyProps,
  dbLoadingProps,
  dbFullfilledProps,
} from "./databaseTables.fixtures";
import { withRouterDecorator } from "src/util/decorators";

storiesOf("DatabaseSummaryTables", module)
  .addDecorator(withRouterDecorator)
  .add("Loading state", () => <DatabaseSummaryTables {...dbLoadingProps} />)
  .add("Empty state", () => <DatabaseSummaryTables {...dbEmptyProps} />)
  .add("Default state", () => <DatabaseSummaryTables {...dbFullfilledProps} />);
