// source: util/chainprop.ts
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * Utils contains common utilities.
 */
module Utils {
  "use strict";

	/**
	 * ChainProperty is a function interface for chainable property setter.
	 * The property itself contains an object of type T, and is part of a
	 * container object of type U.
	 *
	 * When called with no arguments, the property returns the current value of
	 * the property.  When called with an argument of type T, the stored value
	 * is updated and the container object is returned, allowing properties to
	 * be set in a chain.
	 *
	 * Example:
	 *  var obj:chainObj = (new chainObj()).ChainProp1("test").ChainProp2("value");
	 *  var val2:string = obj.ChainProp2(); // "value"
	 */
  export interface ChainProperty<T, U> {
    (): T;
    (arg: T): U;
  }

	/**
	 * ChainProp returns a function which implements the ChainProperty
	 * interface.
	 *
	 * @param _this the object to which this property will be bound. Thi Thi
	 * @param val the default value of the property.
	 */
  export function ChainProp<T, U>(_this: U, val: T): ChainProperty<T, U> {
    let obj: T = val;
    return function(value?: T): any {
      if (value === undefined) {
        return obj;
      }
      obj = value;
      return _this;
    };
  }
}
