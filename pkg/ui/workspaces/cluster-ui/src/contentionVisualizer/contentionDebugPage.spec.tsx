// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import {contentionDebugPageTestProps} from "./contentionDebugPage.fixture";
import {ContentionDebugPage} from "./contentionDebugPage";

describe("Transaction Contention Debug Page", () => {

  it("renders expected page", () => {
    const { getAllByText } = render(
      <MemoryRouter>
        <ContentionDebugPage {...contentionDebugPageTestProps} />
      </MemoryRouter>,
    );

  });
});
