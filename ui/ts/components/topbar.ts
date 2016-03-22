// source: components/topbar.ts

/// <reference path="./health.ts" />
/// <reference path="../../typings/browser.d.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  export module Topbar {
    export function controller(): any {
      return {
        updatedStr: "-",
      };
    }

    interface TopbarInfo {
      title: string|MithrilVirtualElement;
      updated: number;
      // TODO: currently the health indicator causes redraws when undesirable
      // hideHealth is set to true to disable the health indicator and prevent
      // redraws every 2 seconds
      hideHealth?: boolean; // hide the health tracker if true
    }

    export function view(ctrl: any, args: TopbarInfo): MithrilVirtualElement {
      if (args.updated && _.isFinite(args.updated)) {
        ctrl.updatedStr = Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(args.updated)));
      }

      return m(".topbar", [
        m("h2", args.title),
        m(".info-container", [
          (args.hideHealth ? m(".health") : m(".health", ["health:", m.component(Components.Health, {})])),
          m(".last-updated", [ m("strong", "Updated: "), ctrl.updatedStr ]),
        ]),
      ]);
    }
  }

}
