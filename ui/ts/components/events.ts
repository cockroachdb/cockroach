// source: components/events.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/sqlquery.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../models/events.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";
  export module Events {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import MithrilController = _mithril.MithrilController;
    import FilterType = Models.Events.FilterType;
    import Filter = Models.Events.Filter;
    import NormalizedRow = Models.Events.NormalizedRow;

    class SQLController implements MithrilController {
      filters: Filter[] = [
        {selected: true, type: FilterType.RANGE, text: "Range Events"},
        {selected: true, type: FilterType.CLUSTER, text: "Cluster Events"},
        {selected: true, type: FilterType.NODE, text: "Node Events"},
        {selected: true, type: FilterType.DB, text: "Database Events"},
        {selected: true, type: FilterType.TABLE, text: "Table Events"},
        {selected: true, type: FilterType.USER, text: "User Events"},
      ];
    }

    export function controller(): any {}

    export function view(ctrl: SQLController, limit: number): MithrilVirtualElement {
      if (Models.Events.eventSingleton.loaded) {
        Models.Events.eventSingleton.setLimit(limit);

        return m(".event-table-container", [
          m(".table-header", [
            m("a", {config: m.route, href: "/nodes/events"}, m("button.right", "View All")),
            m("h1", "Cluster Events"),
            m("span", _.map(ctrl.filters, function (filter: Filter): MithrilVirtualElement {
              return m(
                "button" + (filter.selected ? ".toggled" : ".untoggled"),
                {
                  onclick: function (): void {
                    filter.selected = !filter.selected;
                  },
                },
                filter.text
              );
            })),
          ]),
          m(".event-table", m("table", m("tbody", _.map(_.take(Models.Events.eventSingleton.normalizedRows, limit), function (row: NormalizedRow): MithrilVirtualElement {
            return m("tr", [
              m("td", m(".icon-info-filled")),
              m("td", m(".timestamp", row.timestamp && row.timestamp.format("YYYY-MM-DD HH:mm:ss"))),
              m("td", m(".message", row.content)),
            ]);
          })))),
        ]);
      } else {
        return null;
      }
    }
  }
}
