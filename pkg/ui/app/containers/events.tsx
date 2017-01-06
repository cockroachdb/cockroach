import * as React from "react";
import * as _ from "lodash";

import { AdminUIState } from "../redux/state";
import { refreshEvents } from "../redux/apiReducers";
import { connect } from "react-redux";
import { TimestampToMoment } from "../util/convert";
import * as eventTypes from "../util/eventTypes";

type Event = Proto2TypeScript.cockroach.server.serverpb.EventsResponse.Event;

export interface EventRowProps {
  event: Event;
}

export function getEventInfo(e: Event) {
  let info: any = JSON.parse(e.info) || {};
  let targetId: number = e.target_id.toNumber();
  let content: React.ReactNode;

  switch (e.event_type) {
    case eventTypes.CREATE_DATABASE:
      content = <span>User {info.User} <strong>created database</strong> {info.DatabaseName}</span>;
      break;
    case eventTypes.DROP_DATABASE:
      let tableDropText: string = `${info.DroppedTables.length} tables were dropped: ${info.DroppedTables.join(", ")}`;
      if (info.DroppedTables.length === 0) {
        tableDropText = "No tables were dropped.";
      } else if (info.DroppedTables.length === 1) {
        tableDropText = `1 table was dropped: ${info.DroppedTables[0]}`;
      }
      content = <span>User {info.User} <strong>dropped database</strong> {info.DatabaseName}.{tableDropText}</span>;
      break;
    case eventTypes.CREATE_TABLE:
      content = <span>User {info.User} <strong>created table</strong> {info.TableName}</span>;
      break;
    case eventTypes.DROP_TABLE:
      content = <span>User {info.User} <strong>dropped table</strong> {info.TableName}</span>;
      break;
    case eventTypes.NODE_JOIN:
      content = <span>Node {targetId} <strong>joined the cluster</strong></span>;
      break;
    case eventTypes.NODE_RESTART:
      content = <span>Node {targetId} <strong>rejoined the cluster</strong></span>;
      break;
    default:
      content = <span>Unknown event type: {e.event_type}</span>;
  }

  return {
    timestamp: TimestampToMoment(e.timestamp).fromNow(),
    content: content,
  };
}

export class EventRow extends React.Component<EventRowProps, {}> {
  render() {
    let { event } = this.props;
    let e = getEventInfo(event);
    return <tr>
      <td><div className="events__message">{e.content}</div></td>
      <td><div className="events__timestamp">{e.timestamp}</div></td>
    </tr>;
  }
}

export interface EventListProps {
  events: Event[];
  refreshEvents: typeof refreshEvents;
};

class EventListState {
  numEvents = 10;
}

export class EventList extends React.Component<EventListProps, EventListState> {
  state = new EventListState();

  componentWillMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  moreEventsClick = () => {
    this.setState({
      numEvents: this.state.numEvents + 10,
    });
  }

  render() {
    let events = this.props.events;
    return <div className="events">
      <table>
        <tbody>
          {_.map(_.take(events, this.state.numEvents), (e: Event, i: number) => {
            return <EventRow event={e} key={i} />;
          })}
          <tr>
            <td className="events__more-link" colSpan={2} onClick={this.moreEventsClick}>More Events</td>
          </tr>
        </tbody>
      </table>
    </div>;
  }
}

let events = (state: AdminUIState): Event[] => state.cachedData.events.data && state.cachedData.events.data.events;

// Connect the EventsList class with our redux store.
let eventsConnected = connect(
  (state: AdminUIState) => {
    return {
      events: events(state),
    };
  },
  {
    refreshEvents,
  }
)(EventList);

export default eventsConnected;
