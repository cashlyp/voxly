import { App } from '@/components/App.tsx';
import { ErrorBoundary } from '@/components/ErrorBoundary.tsx';
import { UserProvider } from '@/state/user.tsx';

function ErrorBoundaryError({ error }: { error: unknown }) {
  const message = error instanceof Error
    ? error.message
    : typeof error === 'string'
      ? error
      : 'Unexpected error';
  return (
    <div className="panel">
      <div className="empty-title">Something went wrong</div>
      <div className="empty-subtitle">{message}</div>
      <button
        type="button"
        className="pill-card"
        onClick={() => window.location.reload()}
      >
        Reload
      </button>
    </div>
  );
}

export function Root() {
  return (
    <ErrorBoundary fallback={ErrorBoundaryError}>
      <UserProvider>
        <App/>
      </UserProvider>
    </ErrorBoundary>
  );
}
