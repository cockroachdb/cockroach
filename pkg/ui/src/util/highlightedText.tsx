import React from "react";

export default function getHighlightedText(text: string, highlight: string) {
  if (highlight.length === 0) {
    return text;
  }
  highlight = highlight.replace(/[°§%()\[\]{}\\?´`'#|;:+-]+/g, "highlightNotDefined");
  const search = highlight.split(" ").map(val => val.toLowerCase()).join("|");
  const parts = text.split(new RegExp(`(${search})`, "gi"));
  return parts.map((part, i) => {
    if (search.includes(part.toLowerCase())) {
      return (
        <span key={i} className="_text-bold">
          {`${part}`}
        </span>
      );
    } else {
      return `${part}`;
    }
  });
}
