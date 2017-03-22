import * as React from "react";
import { Link } from "react-router";
import * as _ from "lodash";
import { connect } from "react-redux";
import * as moment from "moment";

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
   // How long ago the event occurred  (e.g. "10 minutes ago").
  fromNowString: string;
  sortableTimestamp: moment.Moment;
  content: React.ReactNode;
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

let s = (v: any) => JSON.stringify(v, undefined, 2);

export function getEventInfo(e: Event): SimplifiedEvent {
  let info: any = _.isString(e.info) ? JSON.parse(e.info) : {};
  let targetId: number = e.target_id ? e.target_id.toNumber() : null;
  let content: React.ReactNode;

  switch (e.event_type) {
    case eventTypes.CREATE_DATABASE:
      content = <span>Database Created: User {info.User} created database {info.DatabaseName}</span>;
      break;
    case eventTypes.DROP_DATABASE:
      info.DroppedTables = info.DroppedTables || [];
      let tableDropText: string = `${info.DroppedTables.length} tables were dropped: ${info.DroppedTables.join(", ")}`;
      if (info.DroppedTables.length === 0) {
        tableDropText = "No tables were dropped.";
      } else if (info.DroppedTables.length === 1) {
        tableDropText = `1 table was dropped: ${info.DroppedTables[0]}`;
      }
      content = <span>Database Dropped: User {info.User} dropped database {info.DatabaseName}.{tableDropText}</span>;
      break;
    case eventTypes.CREATE_TABLE:
      content = <span>Table Created: User {info.User} created table {info.TableName}</span>;
      break;
    case eventTypes.DROP_TABLE:
      content = <span>Table Dropped: User {info.User} dropped table {info.TableName}</span>;
      break;
    case eventTypes.ALTER_TABLE:
      content = <span>Schema Change: User {info.User} began a schema change to alter table {info.TableName} with ID {info.MutationID}</span>;
      break;
    case eventTypes.CREATE_INDEX:
      content = <span>Schema Change: User {info.User} began a schema change to create an index {info.IndexName} on table {info.TableName} with ID {info.MutationID}</span>;
      break;
    case eventTypes.DROP_INDEX:
      content = <span>Schema Change: User {info.User} began a schema change to drop index {info.IndexName} on table {info.TableName} with ID {info.MutationID}</span>;
      break;
    case eventTypes.CREATE_VIEW:
      content = <span>View Created: User {info.User} created view {info.ViewName}</span>;
      break;
    case eventTypes.DROP_VIEW:
      content = <span>View Dropped: User {info.User} dropped view {info.ViewName}</span>;
      break;
    case eventTypes.REVERSE_SCHEMA_CHANGE:
      content = <span>Schema Change Reversed: Schema change with ID {info.MutationID} was reversed.</span>;
      break;
    case eventTypes.FINISH_SCHEMA_CHANGE:
      content = <span>Schema Change Finished: Schema Change Completed: Schema change with ID {info.MutationID} was completed.</span>;
      break;
    case eventTypes.NODE_JOIN:
      content = <span>Node Joined: Node {targetId} joined the cluster</span>;
      break;
    case eventTypes.NODE_RESTART:
      content = <span>Node Rejoined: Node {targetId} rejoined the cluster</span>;
      break;
    default:
      content = <span>Unknown Event Type: {e.event_type}, content: {s(info)}</span>;
  }

  return {
    fromNowString: TimestampToMoment(e.timestamp).fromNow()
      .replace("second", "sec")
      .replace("minute", "min"),
    content,
    sortableTimestamp: TimestampToMoment(e.timestamp),
  };
}

export class EventRow extends React.Component<EventRowProps, {}> {
  render() {
    let { event } = this.props;
    let e = getEventInfo(event);
    return <tr>
      <td><div className="events__message">{e.content}</div></td>
      <td><div className="events__timestamp">{e.fromNowString}</div></td>
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
        <div className="l-columns__left events-table">
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
                cell: (e) => e.fromNowString,
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
