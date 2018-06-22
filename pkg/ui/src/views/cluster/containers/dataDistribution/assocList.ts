// TODO(vilterp): use a Map FFS
// I guegss it has to be immutable tho??

import _ from "lodash";

interface AssocItem<K, V> {
  key: K;
  value: V;
}

export type AssocList<K, V> = AssocItem<K, V>[];

// my best attempt to not pull in Immutable.js
export function putAssocList<K, V>(
  list: AssocList<K, V>,
  path: K,
  update: (ps: V) => V,
): AssocList<K, V> {
  let found = false;
  const output: AssocList<K, V> = [];
  for (let i = 0; i < list.length; i++) {
    const item = list[i];
    if (_.isEqual(item.key, path)) {
      if (found) {
        throw Error(`dup in putAssocList: ${path}`);
      }
      output.push({
        key: item.key,
        value: update(item.value),
      });
      found = true;
    } else {
      output.push(item);
    }
  }
  if (!found) {
    output.push({
      key: path,
      value: update(null),
    });
  }
  return output;
}

export function getAssocList<K, V>(
  list: AssocList<K, V>,
  path: K,
): V {
  for (let i = 0; i < list.length; i++) {
    const item = list[i];
    if (_.isEqual(item.key, path)) {
      return item.value;
    }
  }
  return null;
}
