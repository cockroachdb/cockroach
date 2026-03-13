// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { allEvents } from "src/util/eventTypes";
import {
  EventBox,
  EventRow,
  getEventInfo,
} from "src/views/cluster/containers/events";

jest.mock("@cockroachlabs/cluster-ui", () => {
  const actual = jest.requireActual("@cockroachlabs/cluster-ui");
  return {
    ...actual,
    api: {
      ...actual.api,
      useEvents: jest.fn(),
    },
  };
});

const mockUseEvents = clusterUiApi.useEvents as jest.MockedFunction<
  typeof clusterUiApi.useEvents
>;

const createEventWithEventType = (
  eventType: string,
): clusterUiApi.EventColumns => {
  return {
    eventType: eventType,
    timestamp: "2016-01-25T10:10:10.555555",
    reportingID: "1",
    info: `{"Timestamp":1668442242840943000,"EventType":"${eventType}","NodeID":1,"StartedAt":1668442242644228000,"LastUp":1668442242644228000}`,
    uniqueID: "\\\x4ce0d9e74bd5480ab1d9e6f98cc2f483",
  };
};

function makeEventBox() {
  return render(
    <MemoryRouter>
      <EventBox />
    </MemoryRouter>,
  );
}

function makeEvent(event: clusterUiApi.EventColumns) {
  return render(
    <table>
      <tbody>
        <EventRow event={event} />
      </tbody>
    </table>,
  );
}

describe("<EventBox>", function () {
  beforeEach(() => {
    mockUseEvents.mockReturnValue({
      data: { results: [], maxSizeReached: false },
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    });
  });

  it("renders without crashing when events are empty", function () {
    makeEventBox();
  });

  it("renders events when data is available", function () {
    mockUseEvents.mockReturnValue({
      data: {
        results: [createEventWithEventType("create_database")],
        maxSizeReached: false,
      },
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    });
    const { container } = makeEventBox();
    expect(
      container.querySelector("div.events__message span")?.textContent,
    ).toContain("created database");
  });
});

describe("<EventRow>", function () {
  describe("attach", function () {
    it("correctly renders a known event", function () {
      const e: clusterUiApi.EventColumns =
        createEventWithEventType("create_database");
      const { container } = makeEvent(e);

      expect(
        container.querySelector("div.events__message span")?.textContent,
      ).toContain("created database");
    });

    it("correctly renders an unknown event", function () {
      const e: clusterUiApi.EventColumns = createEventWithEventType("unknown");
      const { container } = makeEvent(e);

      expect(
        container.querySelector("div.events__message span")?.textContent,
      ).toContain("unknown");
    });
  });
});

describe("getEventInfo", function () {
  it("covers every currently known event", function () {
    allEvents.forEach(eventType => {
      const event: clusterUiApi.EventColumns =
        createEventWithEventType(eventType);
      const { container } = render(
        getEventInfo(event, "UTC").content as React.ReactElement<any>,
      );
      expect(container.textContent).not.toMatch(/Unknown event type/);
    });
  });
});
