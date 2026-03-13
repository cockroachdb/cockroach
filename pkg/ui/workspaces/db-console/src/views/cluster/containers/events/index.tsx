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
} from "@cockroachlabs/cluster-ui";
import { InlineAlert } from "@cockroachlabs/ui-components";
import map from "lodash/map";
import take from "lodash/take";
import moment from "moment-timezone";
import React, { useContext } from "react";
import { Helmet } from "react-helmet";
import { useDispatch, useSelector } from "react-redux";
import { Link } from "react-router-dom";

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { getEventDescription } from "src/util/events";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import "./events.scss";

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

const EventSortedTable = SortedTable<SimplifiedEvent>;

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

export function EventBox(): React.ReactElement {
  const { data } = clusterUiApi.useEvents();
  const events = data?.results;

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

export function EventPage(): React.ReactElement {
  const { data, error } = clusterUiApi.useEvents();
  const events = data?.results;
  const maxSizeApiReached = !!data?.maxSizeReached;
  const timezone = useContext(TimezoneContext);
  const sortSetting = useSelector(eventsSortSetting.selector);
  const dispatch = useDispatch();

  const renderContent = () => {
    const simplifiedEvents = map(events, event => {
      return getEventInfo(event, timezone);
    });

    return (
      <>
        <div className="l-columns__left events-table">
          <EventSortedTable
            data={simplifiedEvents}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting: SortSetting) =>
              dispatch(eventsSortSetting.set(setting))
            }
            columns={[
              {
                title: "Event",
                name: "event",
                cell: (e: SimplifiedEvent) => e.content,
              },
              {
                title: "Timestamp",
                name: "timestamp",
                cell: (e: SimplifiedEvent) => e.fromNowString,
                sort: (e: SimplifiedEvent) => e.sortableTimestamp,
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
  };

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
          error={error}
          render={renderContent}
        />
      </section>
    </div>
  );
}
