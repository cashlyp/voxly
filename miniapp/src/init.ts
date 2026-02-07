import {
  backButton,
  emitEvent,
  initData,
  init as initSDK,
  miniApp,
  mockTelegramEnv,
  retrieveLaunchParams,
  setDebug,
  themeParams,
  viewport,
} from "@tma.js/sdk-react";

export interface InitOptions {
  debug: boolean;
  eruda: boolean;
  mockForMacOS: boolean;
}

/**
 * Initializes the application and configures its dependencies.
 */
export async function init(options: InitOptions): Promise<void> {
  // Set @telegram-apps/sdk-react debug mode and initialize it.
  setDebug(options.debug);
  initSDK();

  // Add Eruda if needed (dev-only).
  if (options.eruda && import.meta.env.DEV) {
    try {
      const { default: eruda } = await import("eruda");
      eruda.init();
      eruda.position({ x: Math.max(0, window.innerWidth - 50), y: 0 });
    } catch (error) {
      console.warn("Failed to initialize Eruda:", error);
    }
  }

  // Telegram for macOS has a ton of bugs, including cases, when the client doesn't
  // even response to the "web_app_request_theme" method. It also generates an incorrect
  // event for the "web_app_request_safe_area" method.
  if (options.mockForMacOS) {
    const launchTheme = retrieveLaunchParams().tgWebAppThemeParams || {};
    mockTelegramEnv({
      onEvent(event, next) {
        if (event.name === "web_app_request_theme") {
          return emitEvent("theme_changed", { theme_params: launchTheme });
        }

        if (event.name === "web_app_request_safe_area") {
          return emitEvent("safe_area_changed", {
            left: 0,
            top: 0,
            right: 0,
            bottom: 0,
          });
        }

        next();
      },
    });
  }

  // Mount all components used in the project.
  backButton.mount.ifAvailable();
  initData.restore();

  if (miniApp.mount.isAvailable()) {
    themeParams.mount();
    miniApp.mount();
    themeParams.bindCssVars();
  }

  if (viewport.mount.isAvailable()) {
    await viewport
      .mount()
      .then(() => {
        viewport.bindCssVars();
      })
      .catch((error) => {
        if (import.meta.env.DEV) {
          console.warn("Failed to mount viewport:", error);
        }
      });
  }
}
