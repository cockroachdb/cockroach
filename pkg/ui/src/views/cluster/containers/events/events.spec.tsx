import React from "react";
import { assert } from "chai";
import { shallow } from "enzyme";
import _ from "lodash";
import Long from "long";
import * as sinon from "sinon";

import * as protos from  "src/js/protos";
import { EventBoxUnconnected as EventBox, EventRow, getEventInfo } from "src/views/cluster/containers/events";
import { refreshEvents } from "src/redux/apiReducers";
import { allEvents } from "src/util/eventTypes";

type Event = protos.cockroach.server.serverpb.EventsResponse.Event;

function makeEventBox(
  events: protos.cockroach.server.serverpb.EventsResponse.Event$Properties[],
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
  return shallow(<EventRow event={event}></EventRow>);
}

describe("<EventBox>", function() {
  let spy: sinon.SinonSpy;

  beforeEach(function() {
    spy = sinon.spy();
  });

  describe("refresh", function() {
    it("refreshes events when mounted.", function () {
      makeEventBox([], spy);
      assert.isTrue(spy.called);
    });
  });

  describe("attach", function() {
    it("attaches event data to contained component", function () {
      const eventsResponse = new protos.cockroach.server.serverpb.EventsResponse({
        events: [
          {
            target_id: Long.fromNumber(1),
            event_type: "test1",
          },
          {
            target_id: Long.fromNumber(2),
            event_type: "test2",
          },
        ],
      });

      const provider = makeEventBox(eventsResponse.events, spy);
      const eventRows = provider.children().first().children().first().children();
      const event1Props: any = eventRows.first().props();
      const event2Props: any = eventRows.at(1).props();
      assert.lengthOf(eventRows, 3); // 3rd row is "more events" link
      assert.isDefined(event1Props.event);
      assert.deepEqual(event1Props.event, eventsResponse.events[0]);
      assert.isDefined(event2Props.event);
      assert.deepEqual(event2Props.event, eventsResponse.events[1]);
    });
  });
});

describe("<EventRow>", function () {
  describe("attach", function () {
    it("correctly renders a known event", function () {
      const e = new protos.cockroach.server.serverpb.EventsResponse.Event({
        target_id: Long.fromNumber(1),
        event_type: "create_database",
      });

      const provider = makeEvent(e);
      assert.lengthOf(provider.first().children(), 2);
      const text = provider.first().childAt(0).text();
      assert(_.includes(text, "created database"));
    });

    it("correctly renders an unknown event", function () {
      const e = new protos.cockroach.server.serverpb.EventsResponse.Event({
        target_id: Long.fromNumber(1),
        event_type: "unknown",
      });

      const provider = makeEvent(e);
      assert.lengthOf(provider.first().children(), 2);
      const text = provider.first().childAt(0).text();
      assert(_.includes(text, "Unknown Event Type"));
    });
  });
});

describe("getEventInfo", function () {
  it("covers every currently known event", function () {
    _.each(allEvents, (eventType) => {
      const event = new protos.cockroach.server.serverpb.EventsResponse.Event({ event_type: eventType });
      const eventContent = shallow(getEventInfo(event).content as React.ReactElement<any>);
      assert.notMatch(eventContent.text(), /Unknown event type/);
    });
  });
});
