/**
 * a DOM helper library.
 * "1 US$ = 18.5842 TR₺ · Oct 16, 2022, 20:52 UTC"
 */

// @ts-check

/// <reference lib="es2022" />
/// <reference lib="dom" />

/**
 * @template TOptions
 * @callback Behavior
 * @param {ParentNode} subtree
 * @param {Partial<TOptions>} [options]
 * @returns {void}
 */

/**
 * @template TOptions
 * @callback BehaviorInit
 * @param {Element} element
 * @param {BehaviorContext<TOptions>} context
 * @returns void
 */

/**
 * @template TOptions
 * @typedef {object} BehaviorContext
 * @property {Root} root
 * @property {Partial<TOptions>} options
 */

/**
 * @typedef {Document | ShadowRoot} Root
 */

/**
 * @typedef {<T>(...args: [..._: unknown[], last: T]) => T} Logger
 */

/**
 * Creates a logging function.
 * The {@link scope} will be prepended to each message logged.
 *
 * We usually use `ilog` as a name for the resulting function.
 * It returns its last argument,
 * which makes it easier to log intermediate values in an expression:
 *
 *     const x = a + ilog("b:", b); // x = a + b
 *
 * @param {string} scope The name of the component/module/etc. that will use this logger.
 * @returns {Logger} The `ilog` function.
 */
export function makelogger(scope) {
  /**
   * @template T
   */
  return (...args) => {
    console.log("%c%s", "color:green", scope, ...args);
    return /** @type {T} */ (args.at(-1));
  };
}

// const ilog = makelogger("19.js");

/**
 * Converts camelCase to kebab-case.
 * @param {string} s
 * @returns {string}
 */
function camelToKebab(s) {
  return s.replace(/[A-Z]/g, (s) => "-" + s.toLowerCase());
}

/**
 * Traverse the DOM forward or backward from a starting point
 * to find an element matching some selector.
 * @param {"next" | "previous"} direction
 * @param {ParentNode} root - The element within which to look for the next element, e.g. a menu.
 * @param {string} selector - The selector to look for, e.g. `"[role=menuitem]"`.
 * @param {Element | null} current - The element to start the search from, e.g. the currently selected menu item.
 *    If missing, the first or last matching element will be returned (depending on search direction).
 * @param {object} [options]
 * @param {boolean} [options.wrap] Whether to wrap around when the end/start of {@link root} is reached.
 */
export function traverse(
  direction,
  root,
  selector,
  current,
  options = {},
) {
  const { wrap = true } = options;

  const advance = /** @type {const} */(`${direction}ElementSibling`);

  const wrapIt = () => {
    // If wrapping is disabled.
    if (!wrap) return null;
    // Wrap in the correct direction.
    return direction === "next"
      ? $(root, selector)
      : $$(root, selector).at(-1);
  }

  if (!current) return wrapIt();

  // Traverse left to right, bottom to top.
  //
  //                                                  (begin ascii art diagram)
  //                           (R)
  //                         /     \
  //                    (r)           (4) <- return value
  //                 /   |   \       /   \
  //    current -> (1)  (2)  (3)    (*) (*)
  //                                                              (end diagram)
  //
  // In the diagram above, 1, 2, 3 are tested by the selector (assuming we
  // start at 1). Then, having run out of siblings, we move up (as many times
  // as needed) before advancing, ending up at 4.
  //
  // To "test" an element, ee call Element#matches, then if that returns false,
  // querySelector. The querySelector call is how the items marked with
  // asterisks can be checked.
  let cursor = current;
  while (true) {
    while (cursor[advance] === null) { // 3
      cursor = /** @type {HTMLElement} */ (cursor.parentElement); // 1 to r
      if (cursor === root) return wrapIt();
    }
    cursor = /** @type {HTMLElement} */ (cursor[advance]); // 1 to 2 to 3, r to 4
    const found = cursor.matches(selector)
      ? cursor // 4
      : $(cursor, selector); // asterisks
    if (found) return found;
  }
}

/**
 * Wrapper for {@linkcode scope}`.querySelector(`{@linkcode sel}`)`.
 * Unlike jQuery, the scope is required to be specified.
 * @template {Element} TElement
 * @param {ParentNode} scope
 * @param {string} sel
 * @returns {TElement | null}
 */
export function $(scope, sel) {
  return scope.querySelector(sel);
}

/**
 * Wrapper for {@linkcode scope}`.querySelectorAll(`{@linkcode sel}`)`.
 * Unlike jQuery, the scope is required to be specified.
 * Returns an Array instead of a NodeList.
 * @template {Element} TElement
 * @param {ParentNode} scope
 * @param {string} sel
 * @returns {TElement[]}
 */
export function $$(scope, sel) {
  return Array.from(scope.querySelectorAll(sel));
}

/**
 * @typedef {object} EventListenerToken
 * Returned by `on`, this is an object you can pass to `off` to remove an event listener,
 * saving the burden of keeping a handle on the listener function and options.
 * @property {EventTarget} target
 * @property {string} type
 * @property {EventListener} listener
 * @property {object} options
 */

/**
 * @template T extends string
 * @callback Listener
 * @param {T extends keyof HTMLElementEventMap ? HTMLElementEventMap[T] : CustomEvent} event
 * @returns void
 */

/**
 * Add an event listener.
 * @template {string} TEventType
 * @param {EventTarget} target The element (or other event target) to add the listener to.
 * @param {TEventType} type The type of event to listen to, e.g. `"click"`.
 * @param {Listener<TEventType>} listener The listener function.
 * @param {object} [options]
 * @param {Element} [options.addedBy] If supplied, the listener will be removed when this element is not in the DOM.
 */
export function on(target, type, listener, options = {}) {
  /** @type {Listener<TEventType>} */
  const listenerWrapper = e => {
    if (options.addedBy && !options.addedBy.isConnected)
      off({ target, type: type, listener: /** @type {EventListener} */ (listenerWrapper), options }); // self-cleaning listener
    if ('logEvents19' in window) console.log(e);
    return listener(e);
  };
  target.addEventListener(type, /** @type {EventListener} */ (listenerWrapper), /** @type {AddEventListenerOptions} */ (options));
  return { target, type: type, options, listener: listenerWrapper };
}

/**
 * Remove an event listener.
 * @param {EventListenerToken} listenerToken - The return value of {@link on}.
 */
export function off({ target, type, listener, options }) {
  return target.removeEventListener(type, listener, options);
}

/**
 * "Halt" an event -- convenient wrapper for `preventDefault`, `stopPropagation`, and `stopImmediatePropagation`.
 * @param {string} o - How to halt. Space-separated list of "default", "bubbling" and "propagation".
 * @param {Event} e - The event.
 */
export function halt(o, e) {
  for (const t of o.split(" ")) {
    if (t === "default")
      e.preventDefault();
    if (t === "bubbling")
      e.stopPropagation();
    if (t === "propagation")
      e.stopImmediatePropagation();
  }
}

/**
 * Decorator for any event listener to call {@link halt}.
 *
 *     on(el, "click", halts("default", e => ...))
 *
 * @template {Event} T
 * @param {string} o - See {@link halt~o}.
 * @param {(e: T) => void} f
 * @returns {(e: T) => void}
 */
export function halts(o, f) {
  return e => { halt(o, e); f(e); };
}

/**
 * Dispatch a {@link CustomEvent}.
 * @param {EventTarget} el - the event target
 * @param {string} type - the event type, e.g. `"myapp:clear-cart"`
 * @param {unknown} [detail] - Event.detail
 * @param {CustomEventInit} [options] CustomEvent constructor options
 */
export function dispatch(el, type, detail, options) {
  return el.dispatchEvent(new CustomEvent(type, { ...options, detail }));
}

/**
 * Get, remove or set an attribute.
 *
 * - attr(el, "name") Get the attribute "name"
 * - attr(el, "name", "value") Set the attribute "name" to "value"
 * - attr(el, "name", null) Remove the attribute "name"
 * - attr(el, [ nameA: "valueA", nameB: "valueB" ]) Set the attributes name-a to "valueA", name-b to "valueB"
 *
 * @param {Element} el
 * @param {string | Record<string, unknown>} name - The attribute name **or** a map of names to values.
 *   If an object is passed, camelCase attribute names will be converted to kebab-case.
 * @param {unknown} value - The value of the attribute, when setting. Pass `null` to remove an attribute.
 * @returns {string | null}
 */
export function attr(el, name, value) {
  if (typeof name === "object") {
    for (const at in name) el.setAttribute(camelToKebab(at), String(name[at]));
    return null;
  } else {
    const curValue = el.getAttribute(name);
    if (value === undefined)
      return el.getAttribute(name);
    else if (value === null)
      return el.removeAttribute(name), curValue;
    else
      return el.setAttribute(name, String(value)), String(value);
  }
}

/**
 * Generate a random ID, copied from `npm:nanoid`.
 */
export function mkid() {
  return Array.from({ length: 21 }, () =>
    'useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict'[
      (Math.random() * 64) | 0
    ]).join("")
}

/**
 * Return an element's ID, setting a generated ID (see {@linkcode mkid}) if it lacks one.
 * @param {Element} el
 */
export function identify(el) {
  if (el.id) return el.id;
  else return el.id = mkid();
}

/**
 * Convert a node to equivalent HTML.
 * @param {Node} node
 * @returns string
 */
export function stringifyNode(node) {
  const tmp = document.createElement("div");
  tmp.append(node);
  return tmp.innerHTML;
}

/**
 * HTML-escape a string.
 * If given a DOM node, it will return **unescaped** HTML for it.
 * Returns empty string when given null or undefined.
 * @param {unknown} s
 * @returns {string}
 */
export function htmlescape(s) {
  if (s === null || s === undefined)
    return "";
  if (s instanceof Node)
    return stringifyNode(s);
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("'", "&#x27;")
    .replaceAll("\"", "&quot;");
}


/**
 * Template literal that escapes HTML in interpolated values and returns a DocumentFragment.
 * Can also be called with a string to parse it as HTML.
 * To let trusted HTML through escaping, parse it first:
 *
 *     html`<p>My trusted markup: ${html(trustedMarkup)}</p>`
 *
 * @param {TemplateStringsArray | string} str
 * @param {...unknown} values
 * @returns {DocumentFragment}
 */
export function html(str, ...values) {
  const tmpl = document.createElement("template");
  if (typeof str === "object" && "raw" in str)
    str = String.raw(str, ...values.map(htmlescape));
  tmpl.innerHTML = str;
  return tmpl.content;
}

/**
 * Create CSSStyleSheet objects. Useful for Custom Elements.
 * @param {TemplateStringsArray | string} str
 * @param {...unknown} values
 * @returns {CSSStyleSheet}
 */
export function css(str, ...values) {
  const ss = new CSSStyleSheet();
  if (typeof str === "object" && "raw" in str)
    str = String.raw(str, ...values);
  ss.replaceSync(str);
  return ss
}

/**
 * Template literal tag to create a CSSStyleDeclaration object.
 * @param {TemplateStringsArray | string} str
 * @param {...unknown} values
 * @returns {CSSStyleDeclaration}
 */
export function style(str, ...values) {
  const sd = new CSSStyleDeclaration();
  if (typeof str === "object" && "raw" in str)
    str = String.raw(str, ...values);
  sd.cssText = str;
  return sd
}

/**
 * 'Type "Element" cannot be assigned to type "HTMLElement"' SHUT UP
 * @param {unknown} el
 * @returns {HTMLElement | null}
 */
export function asHtml(el) {
  return el instanceof HTMLElement ? el : null;
}

/**
 * Find the next element matching a given selector, searching deeply throughout the DOM.
 * @see traverse
 * @param {ParentNode} root The element within which to look for the next element, e.g. a menu.
 * @param {string} selector The selector to look for, e.g. `"[role=menuitem]"`.
 * @param {Element | null} current The element to start the search from, e.g. the currently selected menu item.
 *    If missing, the first or last matching element will be returned (depending on search direction).
 * @param {object} options
 * @param {boolean} [options.wrap] Whether to wrap around when the end/start of {@link root} is reached.
 */
export function next(root, selector, current, options = {}) {
  return traverse("next", root, selector, current, options);
}

/**
 * Find the previous element matching a given selector, searching deeply throughout the DOM.
 * @see traverse
 * @param {ParentNode} root The element within which to look for the next element, e.g. a menu.
 * @param {string} selector The selector to look for, e.g. `"[role=menuitem]"`.
 * @param {Element | null} current The element to start the search from, e.g. the currently selected menu item.
 *    If missing, the first or last matching element will be returned (depending on search direction).
 * @param {object} options
 * @param {boolean} [options.wrap] Whether to wrap around when the end/start of {@link root} is reached.
 */
export function prev(root, selector, current, options = {}) {
  return traverse("previous", root, selector, current, options);
}

/**
 * @callback KeyboardEventListener
 * @param {KeyboardEvent} e
 * @returns {void}
 */

/**
 * Create a handler for keyboard events using a keyboard shortcut DSL.
 *
 * - "ArrowLeft"
 * - "Ctrl+Alt+3"
 *
 * @param {Record<string, KeyboardEventListener>} hotkeys
 * @returns KeyboardEventListener
 */
export function hotkey(hotkeys) {
  const alt = 0b1, ctrl = 0b10, meta = 0b100, shift = 0b1000;
  // handlers[key][modifiers as bitfields]
  /** @type {Record<string, Record<number, KeyboardEventListener>>} */
  const handlers = {};
  const modifiersOf = (/** @type {KeyboardEvent} */ e) =>
    ~~(e.altKey && alt) | ~~(e.ctrlKey && ctrl) |
    ~~(e.metaKey && meta) | ~~(e.shiftKey && shift);
  const parse = /** @returns {[string, number]} */ (/** @type {string} */ hotkeySpec) => {
      const
        tokens = hotkeySpec.split("+"), key = /** @type {string} */ (tokens.pop());
      let modifiers = 0 | 0;
      for (const token of tokens)
        switch (token.toLowerCase()) {
          case "alt": modifiers |= alt; break;
          case "ctrl": modifiers |= ctrl; break;
          case "meta": modifiers |= meta; break;
          case "shift": modifiers |= shift; break;
        }
      return [key, modifiers];
    };

  for (const [hotkeySpec, handler] of Object.entries(hotkeys)) {
    const [key, modifiers] = parse(hotkeySpec);
    (handlers[key] ??= new Array(8))[modifiers] = handler;
  }

  return (/** @type {KeyboardEvent} */ e) => handlers[e.key]?.[modifiersOf(e)]?.(e);
}

/**
 * Debounce a function.
 *
 * @template {unknown[]} TArgs
 * @param {number} t The debounce time.
 * @param {(...args: TArgs) => void} f The function.
 * @param {object} [options]
 * @param {"leading" | "trailing"} [options.mode] Leading or trailing debounce.
 * @returns {(...args: TArgs) => void}
 */
export function debounce(t, f, { mode = "trailing" } = {}) {
  /** @type {number | null} */
  let timeout = null;
  return (/** @type {TArgs} */ ...args) => {
    if (timeout)
      clearTimeout(timeout);
    else if (mode === "leading")
      f(...args);
    timeout = setTimeout(() => {
      if (mode === "trailing")
        f(...args);
      timeout = null;
    }, t);
  };
}

/**
 * Create a behavior that applies to elements matching the given selector.
 * @template TOptions
 * @param {string} selector
 * @param {BehaviorInit<TOptions>} init
 * @returns {Behavior<TOptions>} A function that can be called to apply the behavior to new elements within a subtree.
 */
export function behavior(selector, init) {
  const initialized = new WeakSet;
  return (subtree = document, options = {}) => {
    const root = /** @type {Root} */ (subtree.getRootNode());
    $$(subtree, selector).forEach(el => {
      if (initialized.has(el))
        return;
      initialized.add(el);
      init(el, { options, root });
    });
  };
}

/**
 * @template TData
 * @typedef {object} Repeater
 *
 * @property {(datum: TData) => string} idOf
 * Returns the HTML id for a given data value.
 *
 * @property {(datum: TData, ctx: { id: string }) => ChildNode} create
 * Creates a an element for a data value.
 *
 * @property {(el: Element, datum: TData) => Element | null} update
 * Update an element for a new data value.
 */

/**
 * Repeat an element such that the list can be updated when data changes.
 * @template TData
 * @param {ParentNode} container
 * @param {Repeater<TData>} rep
 */
export function repeater(container, rep) {
  return (/** @type {Iterable<TData>} */ dataset) => {
    /** @type {ChildNode | null} */
    let cursor = null;

    const append = (/** @type {ChildNode[]} */ ...nodes) => {
      const oldcursor = cursor;
      cursor = /** @type {ChildNode} */ (nodes.at(-1));
      if (cursor instanceof DocumentFragment)
        cursor = cursor.lastChild;
      if (oldcursor)
        oldcursor.after(...nodes);
      else
        container.prepend(...nodes);
    };
    const clearAfter = (/** @type {Node | null} */ cursor) => {
      if (cursor)
        while (cursor.nextSibling)
          cursor.nextSibling.remove();
      else
        container.replaceChildren();
    };

    const root = /** @type {Root} */ (container.getRootNode());

    // TODO: use an actual morphing algo
    for (const datum of dataset) {
      const
        id = rep.idOf(datum), existing = root.getElementById(id);
      if (existing)
        append(rep.update?.(existing, datum) ?? existing);
      else
        append(rep.create(datum, { id }));
    }
    clearAfter(cursor);
  };
}
