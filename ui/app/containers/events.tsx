import * as React from "react";
import * as _ from "lodash";

import { AdminUIState } from "../redux/state";
import { refreshEvents } from "../redux/apiReducers";
import { connect } from "react-redux";
import { TimestampToMoment } from "../util/convert";

type Event = cockroach.server.serverpb.EventsResponse.Event;

export interface EventRowProps {
  event: Event;
}

export function getEventInfo(e: Event) {
  let info: any = JSON.parse(e.info) || {};
  let targetId: number = e.target_id.toNumber();
  let content: React.ReactNode;

  switch (e.event_type) {
    case "create_database":
      content = <span>User {info.User} <strong>created database</strong> {info.DatabaseName}</span>;
      break;
    case "drop_database":
      let tableDropText: string = `${info.DroppedTables.length} tables were dropped: ${info.DroppedTables.join(", ")}`;
      if (info.DroppedTables.length === 0) {
        tableDropText = "No tables were dropped.";
      } else if (info.DroppedTables.length === 1) {
        tableDropText = `1 table was dropped: ${info.DroppedTables[0]}`;
      }
      content = <span>User {info.User} <strong>dropped database</strong> {info.DatabaseName}.{tableDropText}</span>;
      break;
    case "create_table":
      content = <span>User {info.User} <strong>created table</strong> {info.TableName}</span>;
      break;
    case "drop_table":
      content = <span>User {info.User} <strong>dropped table</strong> {info.TableName}</span>;
      break;
    case "node_join":
      content = <span>Node {targetId} <strong>joined the cluster</strong></span>;
      break;
    case "node_restart":
      content = <span>Node {targetId} <strong>rejoined the cluster</strong></span>;
      break;
    default:
      content = <span>Unknown event type: {e.event_type}</span>;
  }

  return {
    timestamp: TimestampToMoment(e.timestamp).format("YYYY-MM-DD HH:mm:ss"),
    content: content,
  };
}

export class EventRow extends React.Component<EventRowProps, {}> {
  render() {
    let { event } = this.props;
    let e = getEventInfo(event);
    return <tr>
      <td><div className="icon-info-filled"></div></td>
      <td><div className="timestamp">{e.timestamp}</div></td>
      <td><div className="message">{e.content}</div></td>
    </tr>;
  }
}

export interface EventListProps {
  events: Event[];
  refreshEvents: typeof refreshEvents;
};

export class EventList extends React.Component<EventListProps, {}> {
  componentWillMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  render() {
    let events = this.props.events;
    return <div className="event-table-container">
      <div className="event-table">
        <table>
          <tbody>
            {_.map(events, (e: Event, i: number) => {
              return <EventRow event={e} key={i} />;
          })}
          </tbody>
        </table>
      </div>
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
