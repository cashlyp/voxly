export function SkeletonPanel({ title }: { title?: string }) {
  return (
    <div className="panel skeleton-panel">
      <div className="skeleton-title">
        {title != null && title !== "" ? title : "Loading"}
      </div>
      <div className="skeleton-line" />
      <div className="skeleton-line" />
      <div className="skeleton-line short" />
    </div>
  );
}

export function SkeletonList({ rows = 4 }: { rows?: number }) {
  return (
    <div className="skeleton-list" aria-hidden="true">
      {Array.from({ length: rows }).map((_, index) => (
        <div key={`skeleton-${index}`} className="skeleton-item" />
      ))}
    </div>
  );
}
