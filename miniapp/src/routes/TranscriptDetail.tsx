import {
  Banner,
  Cell,
  Chip,
  Input,
  List,
  Placeholder,
  Section,
  Select,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useMemo, useState } from "react";
import { MaskedPhone } from "../components/MaskedPhone";
import { SkeletonList } from "../components/Skeleton";
import { apiFetch } from "../lib/api";

type TranscriptEntry = {
  id: number;
  speaker: string;
  message: string;
  timestamp: string;
  interaction_count?: number | null;
  personality_used?: string | null;
  confidence_score?: number | null;
};

type CallInfo = {
  call_sid: string;
  status?: string | null;
  phone_number?: string | null;
  created_at?: string | null;
  direction?: string | null;
};

export function TranscriptDetail({ callSid }: { callSid: string }) {
  const [call, setCall] = useState<CallInfo | null>(null);
  const [transcripts, setTranscripts] = useState<TranscriptEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [speakerFilter, setSpeakerFilter] = useState("all");

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [callResponse, transcriptResponse] = await Promise.all([
        apiFetch<{ ok: boolean; call?: CallInfo | null }>(
          `/webapp/calls/${callSid}`,
        ),
        apiFetch<{ ok: boolean; transcripts?: TranscriptEntry[] }>(
          `/webapp/calls/${callSid}/transcripts`,
        ),
      ]);
      setCall(callResponse.call ?? null);
      setTranscripts(transcriptResponse.transcripts ?? []);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load transcript",
      );
    } finally {
      setLoading(false);
    }
  }, [callSid]);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  const filtered = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return transcripts.filter((entry) => {
      if (speakerFilter !== "all" && entry.speaker !== speakerFilter) {
        return false;
      }
      if (normalizedQuery.length === 0) return true;
      return entry.message.toLowerCase().includes(normalizedQuery);
    });
  }, [query, speakerFilter, transcripts]);

  const stats = useMemo(() => {
    const total = transcripts.length;
    const userCount = transcripts.filter((t) => t.speaker === "user").length;
    const aiCount = transcripts.filter((t) => t.speaker === "ai").length;
    return { total, userCount, aiCount };
  }, [transcripts]);

  return (
    <div className="wallet-page">
      {error !== null && error !== "" && (
        <Banner type="inline" header="Error" description={error} />
      )}
      <List className="wallet-list">
        <Section header="Call" className="wallet-section">
          <Cell subtitle="Call ID" description={callSid}>
            Transcript
          </Cell>
          <Cell subtitle="Caller">
            <MaskedPhone value={call?.phone_number ?? "Unknown"} />
          </Cell>
          <Cell
            subtitle="Status"
            after={<Chip mode="mono">{call?.status ?? "-"}</Chip>}
          >
            {call?.direction ?? ""}
          </Cell>
          <Cell subtitle="Started at">{call?.created_at ?? "-"}</Cell>
        </Section>

        <Section header="Filters" className="wallet-section">
          <Input
            header="Search"
            placeholder="Search transcript"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <Select
            header="Speaker"
            value={speakerFilter}
            onChange={(event) => setSpeakerFilter(event.target.value)}
          >
            <option value="all">All speakers</option>
            <option value="user">User</option>
            <option value="ai">AI</option>
          </Select>
          <div className="section-actions">
            <Chip mode="mono">{stats.total} total</Chip>
            <Chip mode="mono">{stats.userCount} user</Chip>
            <Chip mode="mono">{stats.aiCount} ai</Chip>
          </div>
        </Section>

        <Section header="Transcript" className="wallet-section">
          {loading && transcripts.length === 0 ? (
            <SkeletonList rows={6} />
          ) : filtered.length === 0 ? (
            <Placeholder
              header="No transcript entries"
              description="Try adjusting your filters."
            />
          ) : (
            filtered.map((entry) => (
              <Cell
                key={entry.id}
                subtitle={entry.message}
                description={entry.timestamp}
                after={
                  <Chip mode={entry.speaker === "user" ? "mono" : "outline"}>
                    {entry.speaker}
                  </Chip>
                }
              >
                {entry.personality_used ?? "Transcript"}
              </Cell>
            ))
          )}
        </Section>
      </List>
    </div>
  );
}
