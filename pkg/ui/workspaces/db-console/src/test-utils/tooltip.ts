import { expect } from "chai";

export const expectPopperTooltipActivated = () =>
  expect(document.querySelector("[data-popper-placement]")).to.exist;
