/**
 * Environment validation and configuration
 */

export interface EnvironmentConfig {
  apiBase: string;
  isDev: boolean;
  isTesting: boolean;
}

function getOptionalEnvVar(key: string, fallback?: string): string | null {
  const value = import.meta.env[`VITE_${key}`] as string | undefined;
  return value ?? fallback ?? null;
}

export function validateUrl(urlString: string): boolean {
  try {
    new URL(urlString);
    return true;
  } catch {
    return false;
  }
}

export function getEnvironmentConfig(): EnvironmentConfig {
  const apiBase =
    getOptionalEnvVar("API_BASE", "") ??
    String(import.meta.env.VITE_API_BASE ?? "");
  const isDev = import.meta.env.DEV;
  const isTesting =
    typeof process !== "undefined" && process.env.NODE_ENV === "test";

  // Validate API base URL format if provided
  if (apiBase !== "" && !validateUrl(apiBase)) {
    const message = `Invalid API base URL: ${apiBase}. Must be a valid URL.`;
    if (!isDev) {
      throw new Error(message);
    }
    console.warn(message);
  }

  return {
    apiBase,
    isDev,
    isTesting,
  };
}

// Validate at import time to catch issues early
try {
  getEnvironmentConfig();
} catch (error) {
  if (import.meta.env.PROD) {
    throw error;
  }
  if (import.meta.env.DEV) {
    console.warn(
      "Environment validation warning:",
      error instanceof Error ? error.message : String(error),
    );
  }
}
