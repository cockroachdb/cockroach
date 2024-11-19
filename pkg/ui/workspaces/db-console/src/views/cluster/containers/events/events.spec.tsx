// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { mount, shallow } from "enzyme";
import each from "lodash/each";
import React from "react";

import { refreshEvents } from "src/redux/apiReducers";
import { allEvents } from "src/util/eventTypes";
import {
  EventBoxUnconnected as EventBox,
  EventRow,
  getEventInfo,
} from "src/views/cluster/containers/events";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

function makeEventBox(
  events: clusterUiApi.EventsResponse,
  refreshEventsFn: typeof refreshEvents,
) {
  return shallow(
    <EventBox
      events={events}
      refreshEvents={refreshEventsFn}
      eventsValid={true}
    />,
  );
}

function makeEvent(event: clusterUiApi.EventColumns) {
  return mount(<EventRow event={event}></EventRow>);
}

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

describe("<EventBox>", function () {
  const spy = jest.fn();

  describe("refresh", function () {
    it("refreshes events when mounted.", function () {
      makeEventBox([], spy);
      expect(spy).toHaveBeenCalled();
    });
  });
});

describe("<EventRow>", function () {
  describe("attach", function () {
    it("correctly renders a known event", function () {
      const e: clusterUiApi.EventColumns =
        createEventWithEventType("create_database");
      const provider = makeEvent(e);

      expect(
        provider
          .find("div.events__message > span")
          .text()
          .includes("created database"),
      ).toBe(true);
      expect(provider.find(ToolTipWrapper).exists()).toBe(true);
    });

    it("correctly renders an unknown event", function () {
      const e: clusterUiApi.EventColumns = createEventWithEventType("unknown");
      const provider = makeEvent(e);

      expect(
        provider.find("div.events__message > span").text().includes("unknown"),
      ).toBe(true);
      expect(provider.find(ToolTipWrapper).exists()).toBe(true);
    });
  });
});

describe("getEventInfo", function () {
  it("covers every currently known event", function () {
    each(allEvents, eventType => {
      const event: clusterUiApi.EventColumns =
        createEventWithEventType(eventType);
      const eventContent = shallow(
        getEventInfo(event, "UTC").content as React.ReactElement<any>,
      );
      expect(eventContent.text()).not.toMatch(/Unknown event type/);
    });
  });
});
