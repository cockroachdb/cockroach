// source: components/select.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
//

/**
 * Components defines reusable components which may be used on multiple pages,
 * or multiple times on the same page.
 */
module Components {
	"use strict";
	/**
	 * Select is a basic html option select.
	 */
	export module Select {
		import Property = _mithril.MithrilProperty;

		/**
		 * Item represents each option that can be selected. The value is the
		 * internal value representing the option and the text represents
		 * the text that is displayed in the option list.
		 */
		export interface Item {
			value: string;
			text: string;
		}

		/**
		 * Options contains all the info needed by the selector.  The items are
		 * a list of options that can be selected. Selected is a mithril
		 * property containing the currently selected option's value. Set this
		 * to the default value when creating the Select. onChange is any
		 * function that takes a string (the updated value) that will be called
		 * right after the selected option changes.
		 */
		export interface Options {
			items: Item[];
			value: Property<string>;
			onChange: (val: string) => void;
		}

		export function controller(options: Options): Options {
			return options;
		}

		export function view(ctrl: Options): _mithril.MithrilVirtualElement {
			return m("select", { onchange: m.withAttr("value", ctrl.onChange) }, [
				ctrl.items.map(function(item: Item): _mithril.MithrilVirtualElement {
					return m("option", { value: item.value, selected: (item.value === ctrl.value()) }, item.text);
				})
			]);
		}
	}
}
