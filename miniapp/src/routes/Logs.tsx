import {
  Banner,
  Button,
  Cell,
  List,
  Placeholder,
  Section,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useState } from "react";
import { SkeletonList } from "../components/Skeleton";
import { apiFetch } from "../lib/api";
import { useUser } from "../state/user";

type AuditLog = {
  id: number;
  user_id: string;
  action: string;
  call_sid?: string | null;
  metadata?: Record<string, unknown> | null;
  created_at: string;
};

export function Logs() {
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");

  const [logs, setLogs] = useState<AuditLog[]>([]);
  const [cursor, setCursor] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadAudit = useCallback(
    async (nextCursor?: number | null, append = false) => {
      setLoading(true);
      setError(null);
      try {
        const params = new URLSearchParams();
        params.set("limit", "20");
        if (nextCursor !== undefined && nextCursor !== null) {
          params.set("cursor", String(nextCursor));
        }
        const response = await apiFetch<{
          ok: boolean;
          logs?: AuditLog[];
          next_cursor: number | null;
        }>(`/webapp/audit?${params.toString()}`);
        const nextLogs = response.logs ?? [];
        setLogs((prev) => (append ? [...prev, ...nextLogs] : nextLogs));
        setCursor(response.next_cursor ?? null);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load logs");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    if (!isAdmin) return;
    void loadAudit(0, false);
  }, [isAdmin, loadAudit]);

  if (!isAdmin) {
    return (
      <div className="wallet-page">
        <Banner
          type="inline"
          header="Access denied"
          description="Only administrators can view audit logs."
        />
      </div>
    );
  }

  return (
    <div className="wallet-page">
      {error !== null && error !== "" && (
        <Banner type="inline" header="Error" description={error} />
      )}
      <List className="wallet-list">
        <Section header="Audit log" className="wallet-section">
          {loading && logs.length === 0 ? (
            <SkeletonList rows={5} />
          ) : logs.length === 0 ? (
            <Placeholder
              header="No audit entries"
              description="Admin actions will appear here."
            />
          ) : (
            logs.map((entry) => (
              <Cell
                key={entry.id}
                subtitle={entry.created_at}
                description={entry.call_sid ?? entry.user_id}
              >
                {entry.action}
              </Cell>
            ))
          )}
          {cursor !== null && (
            <div className="section-actions">
              <Button
                size="s"
                mode="bezeled"
                disabled={loading}
                onClick={() => void loadAudit(cursor, true)}
              >
                {loading ? "Loading..." : "Load more"}
              </Button>
            </div>
          )}
        </Section>
      </List>
    </div>
  );
}
