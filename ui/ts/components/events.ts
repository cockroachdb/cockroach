// source: components/events.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../models/events.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";
  export module Events {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import NormalizedRow = Models.Events.NormalizedRow;

    export function controller(): any {}

    export function view(ctrl: any, limit: number): MithrilVirtualElement {
      if (!Models.Events.eventSingleton.loaded) {
        Models.Events.eventSingleton.refresh().then((): void => {
          m.redraw();
        });
        return m("");
      }

      if (Models.Events.eventSingleton.loaded) {
        Models.Events.eventSingleton.setLimit(limit);

        return m(".event-table-container", [
          m(".event-table", m("table", m("tbody", _.map(_.take(Models.Events.eventSingleton.normalizedRows, limit), function (row: NormalizedRow): MithrilVirtualElement {
            return m("tr", [
              m("td", m(".icon-info-filled")),
              m("td", m(".timestamp", row.timestamp && row.timestamp.format("YYYY-MM-DD HH:mm:ss"))),
              m("td", m(".message", row.content)),
            ]);
          })))),
        ]);
      } else {
        return m("");
      }
    }
  }
}
