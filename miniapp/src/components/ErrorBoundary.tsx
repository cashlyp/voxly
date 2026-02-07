import {
  Component,
  type ComponentType,
  type GetDerivedStateFromError,
  type PropsWithChildren,
  type ReactNode,
} from "react";

export interface ErrorBoundaryProps extends PropsWithChildren {
  fallback?: ReactNode | ComponentType<{ error: unknown }>;
}

interface ErrorBoundaryState {
  error?: unknown;
  errorInfo?: {
    componentStack: string;
  };
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = {};

  static getDerivedStateFromError: GetDerivedStateFromError<
    ErrorBoundaryProps,
    ErrorBoundaryState
  > = (error) => ({ error });

  componentDidCatch(error: Error, errorInfo: { componentStack: string }): void {
    // Log error for debugging
    if (import.meta.env.DEV) {
      console.error("Error Boundary caught:", error, errorInfo);
    }
    this.setState({ errorInfo });
  }

  render(): React.ReactNode {
    const {
      state: { error },
      props: { fallback: Fallback, children },
    } = this;

    return "error" in this.state ? (
      typeof Fallback === "function" ? (
        <Fallback error={error} />
      ) : (
        Fallback
      )
    ) : (
      children
    );
  }
}
