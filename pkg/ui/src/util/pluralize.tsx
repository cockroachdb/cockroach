export function pluralize(value: number, singular: string, plural: string) {
  if (value === 1) {
    return singular;
  }
  return plural;
}
