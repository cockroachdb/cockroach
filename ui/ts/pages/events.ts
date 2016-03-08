// source: pages/events.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../models/events.ts" />
/// <reference path="../components/events.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module AdminViews {
  "use strict";
  export module Events {
    export module Page {
      import MithrilVirtualElement = _mithril.MithrilVirtualElement;
      import NavigationBar = Components.NavigationBar;

      export function controller(): any {}

      let defaultTargets: NavigationBar.Target[] = [
        {
          view: "Overview",
          route: "",
        },
        {
          view: "Events",
          route: "events",
        },
      ];

      let isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
        return (m.route() === "/nodes/" + t.route);
      };

      function TargetSet(): NavigationBar.TargetSet {
        return {
          baseRoute: "/nodes/",
          targets: Utils.Prop(defaultTargets),
          isActive: isActive,
        };
      }

      export function view(ctrl: any): MithrilVirtualElement {
        return m(".page.events", [
          m.component(Components.Topbar, {title: "Nodes", updated: Utils.Convert.MilliToNano(Models.Events.eventSingleton.updated.valueOf()) }),
          m.component(NavigationBar, {ts: TargetSet()}),
          m(".section", m(Components.Events, 1000)), // TODO: pagination
        ]);
      }
    }
  }
}
