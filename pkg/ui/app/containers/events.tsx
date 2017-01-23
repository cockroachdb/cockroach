import * as React from "react";
import { Link } from "react-router";
import * as _ from "lodash";
import { connect } from "react-redux";
import moment from "moment";

import { AdminUIState } from "../redux/state";
import { refreshEvents } from "../redux/apiReducers";
import { setUISetting } from "../redux/ui";
import { TimestampToMoment } from "../util/convert";
import * as eventTypes from "../util/eventTypes";
import { SortSetting } from "../components/sortabletable";
import { SortedTable } from "../components/sortedtable";

type Event = Proto2TypeScript.cockroach.server.serverpb.EventsResponse.Event;

// Constant used to store sort settings in the redux UI store.
const UI_EVENTS_SORT_SETTING_KEY = "events/sort_setting";
// Number of events to show in the sidebar.
const EVENT_BOX_NUM_EVENTS = 10;

export interface SimplifiedEvent {
  timestamp: string;
  content: React.ReactNode;
  sortableTimestamp: moment.Moment;
}

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const EventSortedTable = SortedTable as new () => SortedTable<SimplifiedEvent>;

export interface EventRowProps {
  event: Event;
}

export function getEventInfo(e: Event): SimplifiedEvent {
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
    sortableTimestamp: TimestampToMoment(e.timestamp),
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

export interface EventBoxProps {
  events: Event[];
  refreshEvents: typeof refreshEvents;
};

export class EventBoxUnconnected extends React.Component<EventBoxProps, {}> {

  componentWillMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  render() {
    let events = this.props.events;
    return <div className="events">
      <table>
        <tbody>
          {_.map(_.take(events, EVENT_BOX_NUM_EVENTS), (e: Event, i: number) => {
            return <EventRow event={e} key={i} />;
          })}
          <tr>
            <td className="events__more-link" colSpan={2}><Link to="/cluster/events">View all events</Link></td>
          </tr>
        </tbody>
      </table>
    </div>;
  }
}

export interface EventPageProps {
  events: Event[];
  refreshEvents: typeof refreshEvents;
  sortSetting: SortSetting;
  setUISetting: typeof setUISetting;
};

export class EventPageUnconnected extends React.Component<EventPageProps, {}> {
  // Callback when the user elects to change the sort setting.
  changeSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_EVENTS_SORT_SETTING_KEY, setting);
  }

  componentWillMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  render() {
    let { events, sortSetting } = this.props;

    let simplifiedEvents = _.map(events, getEventInfo);

    return <div>
      {
        // TODO(mrtracy): This currently always links back to the main cluster
        // page, when it should link back to the dashboard previously visible.
      }
      <section className="section parent-link">
        <Link to="/cluster">&lt; Back to Cluster</Link>
      </section>
      <section className="header header--subsection">
        Events
      </section>
      <section className="section l-columns">
        <div className="l-columns__left">
          <EventSortedTable
            data={simplifiedEvents}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting) => this.changeSortSetting(setting)}
            columns={[
              {
                title: "Event",
                cell: (e) => e.content,
              },
              {
                title: "Timestamp",
                cell: (e) => e.timestamp,
                sort: (e) => e.sortableTimestamp,
              },
            ]}
            />
        </div>
      </section>
    </div>;
  }
}

let events = (state: AdminUIState): Event[] => state.cachedData.events.data && state.cachedData.events.data.events;

// Connect the EventsList class with our redux store.
let eventBoxConnected = connect(
  (state: AdminUIState) => {
    return {
      events: events(state),
    };
  },
  {
    refreshEvents,
  },
)(EventBoxUnconnected);

let sortSetting = (state: AdminUIState): SortSetting => state.ui[UI_EVENTS_SORT_SETTING_KEY] || {};

// Connect the EventsList class with our redux store.
let eventPageConnected = connect(
  (state: AdminUIState) => {
    return {
      events: events(state),
      sortSetting: sortSetting(state),
    };
  },
  {
    refreshEvents,
    setUISetting,
  },
)(EventPageUnconnected);

export { eventBoxConnected as EventBox };
export { eventPageConnected as EventPage };
