/// <reference types="vite/client" />
declare module "eruda" {
  export interface Eruda {
    init(): void;
    position(options: { x: number; y: number }): void;
  }
  const eruda: Eruda;
  // Type is already exported from eruda package
  // export default eruda;
}
