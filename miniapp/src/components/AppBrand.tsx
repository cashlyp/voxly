type AppBrandProps = {
  subtitle?: string;
  meta?: string | null;
  className?: string;
};

export function AppBrand({ subtitle = 'mini app', meta, className }: AppBrandProps) {
  return (
    <div className={['brand', className].filter(Boolean).join(' ')}>
      <div className="brand-title">
        VOICEDNUT
        <span className="brand-badge" aria-hidden="true">
          <span className="brand-check" />
        </span>
      </div>
      {subtitle ? <div className="brand-sub">{subtitle}</div> : null}
      {meta ? <div className="brand-meta">{meta}</div> : null}
    </div>
  );
}
