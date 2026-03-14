import { classNames, isRecord, type ClassNameValue } from '@/css/classnames.js';

export interface BlockFn {
  (...mods: ClassNameValue[]): string;
}

export interface ElemFn {
  (elem: string, ...mods: ClassNameValue[]): string;
}

/**
 * Applies mods to the specified element.
 * @param element - element name.
 * @param mod - mod to apply.
 */
function applyMods(element: string, mod: ClassNameValue): string {
  if (Array.isArray(mod)) {
    return classNames(...mod.map((entry) => applyMods(element, entry)));
  }
  if (isRecord(mod)) {
    return classNames(
      ...Object.entries(mod).map(([modName, value]) => (value ? applyMods(element, modName) : '')),
    );
  }
  const v = classNames(mod);
  return v && `${element}--${v}`;
}

/**
 * Computes final classname for the specified element.
 * @param element - element name.
 * @param mods - mod to apply.
 */
function computeClassnames(element: string, ...mods: ClassNameValue[]): string {
  return classNames(element, applyMods(element, mods));
}

/**
 * @returns A tuple, containing two functions. The first one generates classnames list for the
 * block, the second one generates classnames for its elements.
 * @param block - BEM block name.
 */
export function bem(block: string): [BlockFn, ElemFn] {
  return [
    (...mods) => computeClassnames(block, mods),
    (elem, ...mods) => computeClassnames(`${block}__${elem}`, mods),
  ];
}
