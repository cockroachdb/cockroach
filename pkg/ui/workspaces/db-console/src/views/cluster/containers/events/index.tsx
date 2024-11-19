// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  SortSetting,
  SortedTable,
  util,
  api as clusterUiApi,
  TimezoneContext,
  WithTimezone,
} from "@cockroachlabs/cluster-ui";
import { InlineAlert } from "@cockroachlabs/ui-components";
import map from "lodash/map";
import take from "lodash/take";
import moment from "moment-timezone";
import React, { useContext } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouteComponentProps, withRouter } from "react-router-dom";

import { refreshEvents } from "src/redux/apiReducers";
import {
  eventsLastErrorSelector,
  eventsSelector,
  eventsValidSelector,
  eventsMaxApiReached,
} from "src/redux/events";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { getEventDescription } from "src/util/events";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import "./events.styl";

// Number of events to show in the sidebar.
const EVENT_BOX_NUM_EVENTS = 5;

const eventsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "events/sort_setting",
  s => s.localSettings,
);

export interface SimplifiedEvent {
  // How long ago the event occurred  (e.g. "10 minutes ago").
  fromNowString: string;
  sortableTimestamp: moment.Moment;
  content: React.ReactNode;
}

class EventSortedTable extends SortedTable<SimplifiedEvent> {}

export interface EventRowProps {
  event: clusterUiApi.EventColumns;
}

export function getEventInfo(
  e: clusterUiApi.EventColumns,
  timezone: string,
): SimplifiedEvent {
  return {
    fromNowString: util
      .FormatWithTimezone(
        moment.utc(e.timestamp),
        util.DATE_FORMAT_24_TZ,
        timezone,
      )
      .replace("second", "sec")
      .replace("minute", "min"),
    content: <span>{getEventDescription(e)}</span>,
    sortableTimestamp: moment(e.timestamp),
  };
}

export const EventRow = (props: EventRowProps) => {
  const { event } = props;
  const timezone = useContext(TimezoneContext);
  const e = getEventInfo(event, timezone);
  return (
    <tr>
      <td>
        <ToolTipWrapper placement="left" text={e.content}>
          <div className="events__message">{e.content}</div>
        </ToolTipWrapper>
        <div className="events__timestamp">{e.fromNowString}</div>
      </td>
    </tr>
  );
};

export interface EventBoxProps {
  events: clusterUiApi.EventsResponse;
  // eventsValid is needed so that this component will re-render when the events
  // data becomes invalid, and thus trigger a refresh.
  eventsValid: boolean;
  refreshEvents: typeof refreshEvents;
}

export class EventBoxUnconnected extends React.Component<EventBoxProps, {}> {
  componentDidMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  componentDidUpdate() {
    // Refresh events when props change.
    this.props.refreshEvents();
  }

  render() {
    const events = this.props.events;
    return (
      <div className="events">
        <table>
          <tbody>
            {map(
              take(events, EVENT_BOX_NUM_EVENTS),
              (e: clusterUiApi.EventColumns, i: number) => {
                return <EventRow event={e} key={i} />;
              },
            )}
            <tr>
              <td className="events__more-link" colSpan={2}>
                <Link to="/events">View all events</Link>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  }
}

export interface EventPageProps {
  events: clusterUiApi.EventsResponse;
  // eventsValid is needed so that this component will re-render when the events
  // data becomes invalid, and thus trigger a refresh.
  eventsValid: boolean;
  refreshEvents: typeof refreshEvents;
  sortSetting: SortSetting;
  setSort: typeof eventsSortSetting.set;
  lastError: Error;
  maxSizeApiReached: boolean;
  timezone: string;
}

export class EventPageUnconnected extends React.Component<EventPageProps, {}> {
  componentDidMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  componentDidUpdate() {
    // Refresh events when props change.
    this.props.refreshEvents();
  }

  renderContent() {
    const { events, sortSetting, maxSizeApiReached } = this.props;
    const simplifiedEvents = map(events, event => {
      return getEventInfo(event, this.props.timezone);
    });

    return (
      <>
        <div className="l-columns__left events-table">
          <EventSortedTable
            data={simplifiedEvents}
            sortSetting={sortSetting}
            onChangeSortSetting={setting => this.props.setSort(setting)}
            columns={[
              {
                title: "Event",
                name: "event",
                cell: e => e.content,
              },
              {
                title: "Timestamp",
                name: "timestamp",
                cell: e => e.fromNowString,
                sort: e => e.sortableTimestamp,
              },
            ]}
          />
        </div>
        {maxSizeApiReached && (
          <InlineAlert
            intent="info"
            title={
              <>
                Not all events are displayed because the maximum number of
                events was reached in the console.&nbsp;
              </>
            }
          />
        )}
      </>
    );
  }

  render() {
    const { events, lastError } = this.props;
    return (
      <div>
        <Helmet title="Events" />
        <section className="section section--heading">
          <h1 className="base-heading">Events</h1>
        </section>
        <section className="section">
          <Loading
            loading={!events}
            page={"events"}
            error={lastError}
            render={this.renderContent.bind(this)}
          />
        </section>
      </div>
    );
  }
}

// Connect the EventsList class with our redux store.
const eventBoxConnected = withRouter(
  connect(
    (state: AdminUIState, _: RouteComponentProps) => {
      return {
        events: eventsSelector(state),
        eventsValid: eventsValidSelector(state),
      };
    },
    {
      refreshEvents,
    },
  )(EventBoxUnconnected),
);

// Connect the EventsList class with our redux store.
const eventPageConnected = withRouter(
  connect(
    (state: AdminUIState, _: RouteComponentProps) => {
      return {
        events: eventsSelector(state),
        eventsValid: eventsValidSelector(state),
        sortSetting: eventsSortSetting.selector(state),
        lastError: eventsLastErrorSelector(state),
        maxSizeApiReached: eventsMaxApiReached(state),
      };
    },
    {
      refreshEvents,
      setSort: eventsSortSetting.set,
    },
  )(WithTimezone<EventPageProps>(EventPageUnconnected)),
);

export { eventBoxConnected as EventBox };
export { eventPageConnected as EventPage };
