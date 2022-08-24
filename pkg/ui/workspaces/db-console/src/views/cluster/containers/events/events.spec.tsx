// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { mount, shallow } from "enzyme";
import _ from "lodash";

import * as protos from "src/js/protos";
import {
  EventBoxUnconnected as EventBox,
  EventRow,
  getEventInfo,
} from "src/views/cluster/containers/events";
import { refreshEvents } from "src/redux/apiReducers";
import { allEvents } from "src/util/eventTypes";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

type Event = protos.cockroach.server.serverpb.EventsResponse.Event;

function makeEventBox(
  events: protos.cockroach.server.serverpb.EventsResponse.IEvent[],
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

function makeEvent(event: Event) {
  return mount(<EventRow event={event}></EventRow>);
}

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
      const e = new protos.cockroach.server.serverpb.EventsResponse.Event({
        event_type: "create_database",
      });

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
      const e = new protos.cockroach.server.serverpb.EventsResponse.Event({
        event_type: "unknown",
      });
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
    _.each(allEvents, eventType => {
      const event = new protos.cockroach.server.serverpb.EventsResponse.Event({
        event_type: eventType,
      });
      const eventContent = shallow(
        getEventInfo(event).content as React.ReactElement<any>,
      );
      expect(eventContent.text()).not.toMatch(/Unknown event type/);
    });
  });
});
