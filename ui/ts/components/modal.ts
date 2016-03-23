// source: component/modal.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

module Components {
  "use strict";

  /**
   * Modal is a component that displays a modal
   */
  export module Modal {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    interface ModalConfig {
      // Class applied to the modal and screen container
      containerClass: string;
      // Class applied to the modal
      modalClass: string;

      // Modal title
      title: (string|MithrilVirtualElement|MithrilVirtualElement[]|(MithrilVirtualElement|string)[]);
      // Modal content
      content: (string|MithrilVirtualElement|MithrilVirtualElement[]|(MithrilVirtualElement|string)[]);

      // Close callback for clicking the X or the screen
      onclose: () => void;
    }

    // Mostly used to track the escape key
    class ModalController {

      onclose: () => void;

      // Keyboard event callback that runs the modal close function when the escape key is pressed
      closeOnEscape: (e: KeyboardEvent) => void = (e: KeyboardEvent): void => {
        if (e.keyCode === 27) {
          if (this.onclose) {
            this.onclose();
            m.redraw();
          }
        }
      };

      constructor() {
        // Adds an event listener for the escape key.
        document.addEventListener("keydown", this.closeOnEscape);
      }

      // Removes the event listener if the constructor is unloaded.
      onunload: () => void = (): void => {
        document.removeEventListener("keydown", this.closeOnEscape);
      };
    }

    export function controller(): ModalController {
      return new ModalController();
    }

    export function view(ctrl: ModalController, modalConfig: ModalConfig): _mithril.MithrilVirtualElement {
      // Runs the close function when the escape key is pressed.
      ctrl.onclose = modalConfig.onclose;

      return m(".modal-container" + modalConfig.containerClass, [
        m(".screen", {onclick: modalConfig.onclose}),
        m(".modal" + modalConfig.modalClass, [
          m(".close", {onclick: modalConfig.onclose}, m.trust("&#x2715; ")),
          m("h1.modal-title", modalConfig.title),
          m(".modal-content", modalConfig.content),
        ]),
      ]);
    }
  }
}
