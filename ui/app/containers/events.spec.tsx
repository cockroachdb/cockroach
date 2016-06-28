import * as React from "react";
import { assert } from "chai";
import { shallow } from "enzyme";
import _ = require("lodash");
import Long = require("long");
import * as sinon from "sinon";

import * as protos from  "../js/protos";
import { EventList, EventRow } from "./events";
import { refreshEvents } from "../redux/apiReducers";

type Event = cockroach.server.serverpb.EventsResponse.Event;

function makeEventList(events: Event[], refreshEventsFn: typeof refreshEvents) {
  return shallow(<EventList events={events} refreshEvents={refreshEventsFn}></EventList>);
}

function makeEvent(event: Event) {
  return shallow(<EventRow event={event}></EventRow>);
}

describe("<EventList>", function() {
  let spy: sinon.SinonSpy;

  beforeEach(function() {
    spy = sinon.spy();
  });

  describe("refresh", function() {
    it("refreshes events when mounted.", function () {
      makeEventList((new protos.cockroach.server.serverpb.EventsResponse()).events, spy);
      assert.isTrue(spy.called);
    });
  });

  describe("attach", function() {
    it("attaches event data to contained component", function () {
      let eventsResponse = new protos.cockroach.server.serverpb.EventsResponse({
      events: [
        {
          target_id: Long.fromString("1"),
          event_type: "test1",
        },
        {
          target_id: Long.fromString("2"),
          event_type: "test2",
        },
      ],
    });

      let provider = makeEventList(eventsResponse.events, spy);
      let eventRows = provider.children().first().children().first().children().first().children();
      let event1Props: any = eventRows.first().props();
      let event2Props: any = eventRows.at(1).props();
      assert.lengthOf(eventRows, 2);
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
      let e = new protos.cockroach.server.serverpb.EventsResponse.Event({
        target_id: Long.fromString("1"),
        event_type: "create_database",
      });

      let provider = makeEvent(e);
      assert.lengthOf(provider.first().children(), 3);
      let text = provider.first().childAt(2).text();
      assert(_.includes(text, "created database"));
    });

    it("correctly renders an unknown event", function () {
      let e = new protos.cockroach.server.serverpb.EventsResponse.Event({
        target_id: Long.fromString("1"),
        event_type: "unknown",
      });

      let provider = makeEvent(e);
      assert.lengthOf(provider.first().children(), 3);
      let text = provider.first().childAt(2).text();
      assert(_.includes(text, "Unknown event type"));
    });
  });
});
