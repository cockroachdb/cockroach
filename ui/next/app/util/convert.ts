/**
 * NanosToMilli converts a nanoseconds to milliseconds, for use in Java
 * time methods.
 */
export function NanoToMilli(nano: number): number {
  return nano / 1.0e6;
}
