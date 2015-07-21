// source: util/property.ts
// Author: Matt Tracy (matt@cockroachlabs.com)

module Utils {
  "use strict";

  /**
   * ReadOnlyProperty is a function that returns a value. 
   *
   * Each property also has an Epoch() function which returns a monotonically
   * increasing number indicating the version of the property; if the value
   * returned by the property changes, the epoch value will be increased.
   */
  export interface ReadOnlyProperty<T> {
    (): T;
    // Epoch returns the current update epoch for the property. This is a
    // numeric value that monotonically increases each time the property is
    // updated.
    Epoch(): number;
  }

  /**
   * A Property is a function which returns a value, but can also be called with
   * a new value. This is intended to be used as a getter/setter.
   *
   * The property has the same Epoch() function as a ReadOnlyProperty - the
   * epoch value will be increased every time the property value is set.
   *
   * The property also has an Update() function, which can be called to manually
   * increase the value returned by Epoch(). This is useful for mutable data
   * types like arrays, which may be modified without explicitly calling the
   * setter.
   */
  export interface Property<T> extends ReadOnlyProperty<T> {
    (arg: T): T;
    // Update increments the epoch manually; useful for certain data types like
    // arrays, which may be modified without setting the property.
    Update(): void;
  }

  /**
   * Prop creates a new property with the given initial value.
   */
  export function Prop<T>(initial: T): Property<T> {
    // obj stores the value of the property.
    let obj: T = initial;

    // epoch is a simple number which is incremented whenever the prop is set or
    // when update() is called.
    let epoch: number = 0;

    let propFn: any = function(value?: T): T {
      if (value === undefined) {
        return obj;
      }
      obj = value;
      epoch++;
      return obj;
    };

    propFn.Epoch = () => epoch;
    propFn.Update = function(): void { epoch++; };
    return propFn;
  }

  /**
   * Computed creates a new computed property, whose value is lazily computed
   * based on the values of up to four parent properties.
   *
   * The constructor accepts the parent properties and a computation function;
   * the computation function is used to calculate the value of the computed
   * property based on the current values of all parent properties.
   *
   * The value returned by the Computed property is memoized; it is lazily 
   * re-computed if any of the parent properties have changed since the last
   * access.
   *
   * The value of a computed property cannot be set directly.
   */
  export function Computed<T1, TResult>(p1: ReadOnlyProperty<T1>, fn: (v1: T1) => TResult): ReadOnlyProperty<TResult>;
  export function Computed<T1, T2, TResult>(p1: ReadOnlyProperty<T1>, p2: ReadOnlyProperty<T2>, fn: (v1: T1, v2: T2) => TResult): ReadOnlyProperty<TResult>;
  export function Computed<T1, T2, T3, TResult>(p1: ReadOnlyProperty<T1>, p2: ReadOnlyProperty<T2>, p3: ReadOnlyProperty<T3>, fn: (v1: T1, v2: T2, v3: T3) => TResult): ReadOnlyProperty<TResult>;
  export function Computed<T1, T2, T3, T4, TResult>(p1: ReadOnlyProperty<T1>, p2: ReadOnlyProperty<T2>, p3: ReadOnlyProperty<T3>, p4: ReadOnlyProperty<T4>, fn: (v1: T1, v2: T2, v3: T3, v4: T4) => TResult): ReadOnlyProperty<TResult>;
  export function Computed(...args: any[]): any {
    let obj = null;
    let lastProcessedEpoch = -1;

    // Only the overloads can be called; typescript will ensure we have the
    // correct argument types here.
    let fn = args.pop();
    let parentProps = args;

    // epoch is the sum of all parent epochs. This will increase if any parent
    // property is updated.
    let epochFn = function(): number {
      let sum: number = 0;
      parentProps.forEach((p: Property<any>) => sum += p.Epoch());
      return sum;
    };

    let propFn: any = function(value?: any): any {
      let epoch = epochFn();
      if (epoch > lastProcessedEpoch) {
        // retrieve parent values.
        let values: any[] = parentProps.map((p: Property<any>) => p());

        // pass parent values to computation function and store in the inner
        // property.
        obj = fn.apply(this, values);
        lastProcessedEpoch = epoch;
      }
      return obj;
    };

    propFn.Epoch = epochFn;
    return propFn;
  }
}

