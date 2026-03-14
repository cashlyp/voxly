export function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}

export type ClassNameValue =
  | string
  | number
  | boolean
  | null
  | undefined
  | Record<string, unknown>
  | ClassNameValue[];

/**
 * Function which joins passed values with space following these rules:
 * 1. If value is non-empty string, it will be added to output.
 * 2. If value is object, only those keys will be added, which values are truthy.
 * 3. If value is array, classNames will be called with this value spread.
 * 4. All other values are ignored.
 *
 * You can find this function to similar one from the package {@link https://www.npmjs.com/package/classnames|classnames}.
 * @param values - values array.
 * @returns Final class name.
 */
export function classNames(...values: ClassNameValue[]): string {
  return values
    .map((value) => {
      if (typeof value === 'string') {
        return value;
      }

      if (isRecord(value)) {
        return classNames(
          ...Object.entries(value).map(([entryKey, entryValue]) => (entryValue ? entryKey : '')),
        );
      }

      if (Array.isArray(value)) {
        return classNames(...value);
      }

      if (typeof value === 'number') {
        return String(value);
      }

      return '';
    })
    .filter(Boolean)
    .join(' ');
}

/**
 * Merges two sets of classnames.
 *
 * The function expects to pass an array of objects with values that could be passed to
 * the `classNames` function.
 * @returns An object with keys from all objects with merged values.
 * @see classNames
 */
export function mergeClassNames(
  ...partials: Array<Record<string, ClassNameValue>>
): Record<string, string> {
  return partials.reduce<Record<string, string>>((acc, partial) => {
    if (isRecord(partial)) {
      Object.entries(partial).forEach(([key, value]) => {
        const className = classNames(acc[key], value);
        if (className) {
          acc[key] = className;
        }
      });
    }
    return acc;
  }, {});
}
