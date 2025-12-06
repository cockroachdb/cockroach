/// <reference lib="es2022" />

import { $, $$, on, dispatch, halts, attr, next, prev, asHtml, hotkey, behavior, makelogger } from "./19.js"

const ilog = makelogger("menu");
const sMenu = "[role=menu]";
const sMenuitem = "[role=menuitem]";

/**
 * @param {HTMLElement} menu
 * @returns {HTMLElement[]}
 */
const menuItems = menu => $$(menu, sMenuitem);

/**
 * @param {Element} button 
 * @param {object} options 
 * @param {import("./19.js").Root} options.root 
 * @returns {HTMLElement | null}
 */
const menuOf = (button, { root }) => {
    const id = attr(button, "aria-controls");
    if (id === null) return null;
    return root.getElementById(id);
}

/**
 * @param {HTMLElement} menu
 * @returns {HTMLElement | null}
 */
const firstItem = menu => $(menu, sMenuitem)

/**
 * @param {HTMLElement} menu
 * @returns {HTMLElement | null}
 */
const lastItem = menu => menuItems(menu).at(-1) ?? null;


/**
 * @param {HTMLElement} menu 
 * @returns {boolean}
 */
const isOpen = menu => !menu.hidden;

export const menu = behavior(sMenu, (menu, { root }) => {
    if (!(menu instanceof HTMLElement)) return;

    let opener;

    menuItems(menu).forEach(item => item.setAttribute("tabindex", "-1"));

    on(menu, "menu:open", e => {
        opener = e.detail?.opener;
        if (!opener) ilog("Warning: Menu", menu, "opened without passing an `opener` element");
        menu.hidden = false;
        firstItem(menu)?.focus();
    });

    on(menu, "menu:close", _ => {
        ilog("menu:close", menu.hidden = true);
        opener?.focus();
    });

    on(menu, "focusout", e => {
        if (!isOpen(menu)) return;
        if (menu.contains(/** @type {Node} */(e.relatedTarget))) return;
        if (opener === e.relatedTarget) return;
        dispatch(menu, "menu:close");
    });

    on(menu, "keydown", halts("default", hotkey({
        "ArrowUp": _ => asHtml(prev(menu, sMenuitem, root.activeElement, {}))?.focus(),
        "ArrowDown": _ => asHtml(next(menu, sMenuitem, root.activeElement, {}))?.focus(),
        "Space": _ => asHtml(root.activeElement?.closest(sMenuitem))?.click(),
        "Home": _ => firstItem(menu)?.focus(),
        "End": _ => lastItem(menu)?.focus(),
        "Escape": _ => dispatch(menu, "menu:close"),
    })));

    on(window, "click", e => {
        if (!isOpen(menu)) return;
        if (opener === e.target) return;
        dispatch(menu, "menu:close");
    }, { addedBy: menu });
});

export const menuButton = behavior("[aria-haspopup=menu]", (button, { root }) => {
    const menu = menuOf(button, { root });

    if (menu === null) return ilog("Error: Menu button", button, "has no menu.");

    on(menu, "menu:close", _ => attr(button, "aria-expanded", "false"), { addedBy: button })
    on(menu, "menu:open", _ => attr(button, "aria-expanded", "true"), { addedBy: button })
    on(button, "click", () => dispatch(menu, isOpen(menu) ? "menu:close" : "menu:open", { opener: button }));
});

menu(document);
menuButton(document);
