export function SkeletonPanel({ title }: { title?: string }) {
  return (
    <div className="panel skeleton-panel">
      <div className="skeleton-title">{title || 'Loading'}</div>
      <div className="skeleton-line" />
      <div className="skeleton-line" />
      <div className="skeleton-line short" />
    </div>
  );
}
