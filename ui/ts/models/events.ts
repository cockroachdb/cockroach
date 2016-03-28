// source: models/events.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/api.ts" />
/// <reference path="../models/proto.ts" />

module Models {
  "use strict";

  export module Events {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import Moment = moment.Moment;
    import ClusterEvent = Models.API.ClusterEvent;
    import ClusterEvents = Models.API.ClusterEvents;

    export enum FilterType {
      RANGE, CLUSTER, NODE, DB, TABLE, USER
    }

    enum Icon {
      INFO, WARNING, ERROR, FATAL
    }

    export interface Filter {
      text: string;
      type: FilterType;
      selected: boolean;
    }

    export interface NormalizedRow {
      icon: Icon;
      timestamp: Moment;
      content: MithrilVirtualElement;
    }

    let eventTemplates: { [eventType: string]: (eventRow: ClusterEvent) => NormalizedRow; } = {
      "create_database": (eventRow: ClusterEvent): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`User ${eventRow.info.User} `, m("strong", "created database "), eventRow.info.DatabaseName]),
        };
      },
      "drop_database": (eventRow: ClusterEvent): NormalizedRow => {
        // TODO: truncate table list
        let tableDropText: string = `${eventRow.info.DroppedTables.length} tables were dropped: ${eventRow.info.DroppedTables.join(", ")}`;

        if (eventRow.info.DroppedTables.length === 0) {
          tableDropText = "No tables were dropped.";
        } else if (eventRow.info.DroppedTables.length === 1) {
          tableDropText = `1 table was dropped: ${eventRow.info.DroppedTables[0]}`;
        }

        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [
            `User ${eventRow.info.User} `,
            m("strong", "dropped database "),
            `${eventRow.info.DatabaseName}. `,
            tableDropText,
          ]),
        };
      },
      "create_table": (eventRow: ClusterEvent): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`User ${eventRow.info.User} `, m("strong", "created table "), eventRow.info.TableName]),
        };
      },
      "drop_table": (eventRow: ClusterEvent): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`User ${eventRow.info.User} `, m("strong", "dropped table "), eventRow.info.TableName]),
        };
      },
      "node_join": (eventRow: ClusterEvent): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`Node ${eventRow.target_id} `, m("strong", "joined the cluster")]),
        };
      },
      "node_restart": (eventRow: ClusterEvent): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`Node ${eventRow.target_id} `, m("strong", "rejoined the cluster")]),
        };
      },
    };

    export class Events {

      normalizedRows: NormalizedRow[];
      loaded: boolean = false;
      updated: Moment;
      limit: number = 10;

      constructor() {}

      setLimit(limit: number): void {
        if (this.limit !== limit) {
          this.limit = limit;
          this.refresh();
        }
      }

      refresh(): _mithril.MithrilPromise<any> {
        return Models.API.events().then((response: ClusterEvents): void => {
          this.loaded = true;
          this.normalizedRows = this.convertToNormalizedRows(response.events);
        });
      }

      convertToNormalizedRows(events: ClusterEvent[]): NormalizedRow[] {
        let normalizedEvents: NormalizedRow[] = _.map(events, (e: ClusterEvent): NormalizedRow => {
          return eventTemplates[e.event_type] && eventTemplates[e.event_type](e)
            || {icon: Icon.WARNING, timestamp: moment.utc(e.timestamp), content: m("span", "Unknown event type: " + e.event_type)};
        });

        return _.orderBy(normalizedEvents, ["timestamp"], ["desc"]);
      }
    }

    export let eventSingleton: Events = new Events();
  }
}
