// Include Telegram UI styles first to allow our code override the package CSS.
import "@telegram-apps/telegram-ui/dist/styles.css";

import { retrieveLaunchParams } from "@tma.js/sdk-react";
import { StrictMode } from "react";
import ReactDOM from "react-dom/client";

import { EnvUnsupported } from "@/components/EnvUnsupported.tsx";
import { Root } from "@/components/Root.tsx";
import { init } from "@/init.ts";

import "./index.css";

// Mock the environment in case, we are outside Telegram.
import "./mockEnv.ts";

const rootElement = document.getElementById("root");
if (!rootElement) {
  throw new Error("Root element not found");
}

const root = ReactDOM.createRoot(rootElement);

async function start(): Promise<void> {
  try {
    const launchParams = retrieveLaunchParams();
    const { tgWebAppPlatform: platform } = launchParams;
    const debug =
      (launchParams.tgWebAppStartParam || "").includes("debug") ||
      import.meta.env.DEV;
    const enableEruda =
      import.meta.env.DEV && debug && ["ios", "android"].includes(platform);

    await init({
      debug,
      eruda: enableEruda,
      mockForMacOS: platform === "macos",
    });

    root.render(
      <StrictMode>
        <Root />
      </StrictMode>,
    );
  } catch (error) {
    console.error("Failed to initialize app:", error);
    root.render(<EnvUnsupported />);
  }
}

void start();
