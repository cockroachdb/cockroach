// e.g. intersperse(["foo", "bar", "baz"], "-") => ["foo", "-", "bar", "-", "baz"]
export function intersperse<T>(array: T[], sep: T): T[] {
  const output = [];
  for (let i = 0; i < array.length; i++) {
    if (i > 0) {
      output.push(sep);
    }
    output.push(array[i]);
  }
  return output;
}
