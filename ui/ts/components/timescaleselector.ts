// source: component/timescaleselector.ts
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../util/convert.ts" />
/// <reference path="../models/timescale.ts" />

module Components {
  "use strict";

  /**
   * TimescaleSelector is a component that allows the graph timescale to be changed
   */
  export module TimescaleSelector {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import Timescale = Models.Timescale.Timescale;
    export function controller(): { showTimescales: boolean; } {
      return {
        showTimescales: false,
      };
    }

    export function view(ctrl: { showTimescales: boolean; }): _mithril.MithrilVirtualElement {
      return m(".timescale-selector-container", [
        m("button.timescale", { onclick: (): void => { ctrl.showTimescales = !ctrl.showTimescales; } }, "Select Timescale"),
        m(".timescale-selector" + (ctrl.showTimescales ? ".show" : ""), [m(".text", "View Last: ")].concat(_.map(Models.Timescale.timescales, (t: Timescale): MithrilVirtualElement => {
            return m(
              "button" + (t.selected ? ".selected" : ""),
              { onclick: (): void => { Models.Timescale.setTimescale(t); } },
              t.name);
        }))),
      ]);
    }
  }
}
