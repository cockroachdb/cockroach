// source: models/events.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/sqlquery.ts" />
/// <reference path="../../typings/browser.d.ts" />

module Models {
  "use strict";

  export module Events {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import Moment = moment.Moment;

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

    interface EventInfo {
      DatabaseName?: string;
      TableName?: string;
      User?: string;
      Statement?: string;
      DroppedTables?: string[];
    }

    interface EventRow {
      event: string;
      timestamp: string;
      eventType: string;
      targetID: number;
      reportingID: number;
      info: EventInfo;
    }

    let eventTemplates: { [eventType: string]: (eventRow: EventRow) => NormalizedRow; } = {
      "create_database": (eventRow: EventRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`User ${eventRow.info.User} `, m("strong", "created database "), eventRow.info.DatabaseName]),
        };
      },
      "drop_database": (eventRow: EventRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [
            `User ${eventRow.info.User} `,
            m("strong", "dropped database "),
            `${eventRow.info.DatabaseName}. The following tables were dropped: ${eventRow.info.DroppedTables.join(", ")}`,
          ]), // TODO: truncate table list
        };
      },
      "create_table": (eventRow: EventRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`User ${eventRow.info.User} `, m("strong", "created table "), eventRow.info.TableName]),
        };
      },
      "drop_table": (eventRow: EventRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(eventRow.timestamp),
          content: m("span", [`User ${eventRow.info.User} `, m("strong", "dropped table "), eventRow.info.TableName]),
        };
      },
    };

    interface RangeRow {
      range: string;
      timestamp: string;
      rangeID: number;
      storeID: number;
      eventType: string;
      otherRangeID: number;
      info: any;
    }

    let rangeTemplates: { [rangeType: string]: (rangeRow: RangeRow) => NormalizedRow; } = {
      "split": (rangeRow: RangeRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(rangeRow.timestamp),
          content: m("span", [`Range ${rangeRow.rangeID} was `, m("strong", "split"), ` to create range ${rangeRow.otherRangeID}`]),
        };
      },
      "add": (rangeRow: RangeRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(rangeRow.timestamp),
          content: m("span", [`Range ${rangeRow.rangeID} was `, m("strong", "added")]),
        };
      },
      "remove": (rangeRow: RangeRow): NormalizedRow => {
        return {
          icon: Icon.INFO,
          timestamp: moment.utc(rangeRow.timestamp),
          content: m("span", [`Range ${rangeRow.rangeID} was `, m("strong", "removed")]),
        };
      },
    };

    export class Events {

      normalizedRows: NormalizedRow[];
      loaded: boolean = false;
      updated: Moment;
      limit: number = 10;

      constructor() {
        this.refresh().then((): void => {
          this.loaded = true;
        }) ;
      }

      setLimit(limit: number): void {
        if (this.limit !== limit) {
          this.limit = limit;
          this.refresh();
        }
      }

      refresh(): _mithril.MithrilPromise<any> {
        return m.sync([
          Models.SQLQuery.runQuery(`SELECT timestamp, rangeID , storeID , eventType , otherRangeID, info FROM SYSTEM.RANGELOG ORDER BY timestamp DESC LIMIT ${this.limit};`, true),
          Models.SQLQuery.runQuery(`SELECT timestamp, eventType, targetID, reportingID, info FROM SYSTEM.EVENTLOG ORDER BY timestamp DESC LIMIT ${this.limit};`, true),
        ]).then((promises: any[]): void => {
          _.each(promises[0].concat(promises[1]), function(e: any): void {
            e.info = JSON.parse(e.info);
          });
          this.normalizedRows = this.convertToNormalizedRows(<RangeRow[]>promises[0], <EventRow[]>promises[1]);
          this.updated = moment();
        });
      }

      convertToNormalizedRows(rangeEvents: RangeRow[], events: EventRow[]): NormalizedRow[] {
        let normalizedRangeEvents: NormalizedRow[] = _.map(rangeEvents, (e: RangeRow): NormalizedRow => {
          return rangeTemplates[e.eventType](e) || {icon: Icon.WARNING, timestamp: moment.utc(e.timestamp), content: m("span", "Unknown range event type: " + e.eventType)};
        });

        let normalizedEvents: NormalizedRow[] = _.map(events, (e: EventRow): NormalizedRow => {
          return eventTemplates[e.eventType](e) || {icon: Icon.WARNING, timestamp: moment.utc(e.timestamp), content: m("span", "Unknown event type: " + e.eventType)};
        });

        return _.orderBy(normalizedEvents.concat(normalizedRangeEvents), ["timestamp"], ["desc"]);
      }
    }

    export let eventSingleton: Events = new Events();
  }
}
