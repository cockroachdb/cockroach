// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/lodash/lodash.d.ts" />
/// <reference path="../util/property.ts" />

// Author: Matt Tracy (matt@cockroachlabs.com)

module Components {
  "use strict";

  /**
   * NavigationBar is a general purpose component which generates a list of
   * links. One of the links can be active.
   */
  export module NavigationBar {
    export interface Target {
      title: string;
      route: string;
    }

    export interface TargetSet {
      baseRoute: string;
      targets: Utils.ReadOnlyProperty<Target[]>;
      /**
       * isActive is a function which determines whether a given target is
       * active. 
       */
      isActive: (t: Target) => boolean;
    }

    export function controller(ts: TargetSet): any {}

    export function view(ctrl: any, ts: TargetSet): _mithril.MithrilVirtualElement {
      return m("ul.navigation", _.map(ts.targets(), (t: Target) =>
        m("li",
          {
            className: ts.isActive(t) ? "active" : ""
          },
          m("a",
            {
              config: m.route,
              href: ts.baseRoute + t.route
            },
            t.title)
         )
      ));
    }
  }
}
