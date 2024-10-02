// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import ReactDomServer from "react-dom/server";

import { getHighlightedText } from "./highlightedText";

function elementToString(value: string | JSX.Element): string {
  if (typeof value == "string") {
    return value;
  }
  return ReactDomServer.renderToString(value);
}

describe("Highlighted Text", () => {
  it("text with no highlight", () => {
    const highlightedText = getHighlightedText(
      "full text",
      "no matches",
      false,
      true,
    );
    expect(highlightedText).toEqual(["full text"]);
  });

  it("text with everything highlighted", () => {
    const highlightedText = getHighlightedText(
      "everything matches",
      "everything matches",
      false,
      true,
    );

    expect(highlightedText.length).toEqual(5);
    expect(elementToString(highlightedText[1])).toEqual(
      '<span class="_text-bold" data-reactroot="">everything</span>',
    );
    expect(elementToString(highlightedText[3])).toEqual(
      '<span class="_text-bold" data-reactroot="">matches</span>',
    );
  });

  it("text with partial highlight", () => {
    const highlightedText = getHighlightedText(
      "regular text highlighted match",
      "highlighted match",
      false,
      true,
    );
    expect(highlightedText.length).toEqual(5);
    expect(highlightedText[0].toString()).toEqual("regular text ");
    expect(elementToString(highlightedText[1])).toEqual(
      '<span class="_text-bold" data-reactroot="">highlighted</span>',
    );
    expect(elementToString(highlightedText[3])).toEqual(
      '<span class="_text-bold" data-reactroot="">match</span>',
    );
  });

  it("special characters (used on regex) don't get highlighted", () => {
    const highlightedText = getHighlightedText(
      "text * ? + \\ - ^ () {} matches",
      "* ? + \\ - ^ { } ( )",
      false,
      true,
    );
    expect(highlightedText.length).toEqual(1);
    expect(highlightedText[0].toString()).toEqual(
      "text * ? + \\ - ^ () {} matches",
    );
  });
});
