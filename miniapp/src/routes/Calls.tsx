import {
  Banner,
  Button,
  Cell,
  InlineButtons,
  Input,
  List,
  Section,
  Select,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useState } from "react";
import { MaskedPhone } from "../components/MaskedPhone";
import { SkeletonList } from "../components/Skeleton";
import { navigate } from "../lib/router";
import { loadUiState, updateCallsFilters } from "../lib/uiState";
import { useCalls } from "../state/calls";

function formatUpdatedAt(timestamp: number | null) {
  if (timestamp === null) return "";
  const diffMs = Date.now() - timestamp;
  if (diffMs < 60000) return "Updated just now";
  if (diffMs < 3600000) return `Updated ${Math.floor(diffMs / 60000)}m ago`;
  if (diffMs < 86400000) return `Updated ${Math.floor(diffMs / 3600000)}h ago`;
  return `Updated ${Math.floor(diffMs / 86400000)}d ago`;
}

export function Calls() {
  const { calls, fetchCalls, nextCursor, callsMeta } = useCalls();
  const saved = loadUiState().callsFilters;
  const [statusFilter, setStatusFilter] = useState(saved?.status ?? "");
  const [search, setSearch] = useState(saved?.query ?? "");
  const [cursor, setCursor] = useState(saved?.cursor ?? 0);

  const loadCalls = useCallback(
    async (nextCursorValue = 0) => {
      await fetchCalls({
        limit: 20,
        cursor: nextCursorValue,
        status: statusFilter !== "" ? statusFilter : undefined,
        q: search !== "" ? search : undefined,
      });
    },
    [fetchCalls, statusFilter, search],
  );

  useEffect(() => {
    void loadCalls(0);
  }, [loadCalls]);

  useEffect(() => {
    updateCallsFilters({ status: statusFilter, query: search, cursor });
  }, [statusFilter, search, cursor]);

  const handleNext = () => {
    if (nextCursor !== null) {
      setCursor(nextCursor);
      void loadCalls(nextCursor);
    }
  };

  const handlePrev = () => {
    const prev = Math.max(0, cursor - 20);
    setCursor(prev);
    void loadCalls(prev);
  };

  const handleClear = () => {
    setStatusFilter("");
    setSearch("");
    setCursor(0);
    void loadCalls(0);
  };

  const footerParts = [];
  if (callsMeta.refreshing) footerParts.push("Refreshing");
  if (callsMeta.stale && !callsMeta.refreshing)
    footerParts.push("Showing cached data");
  if (callsMeta.updatedAt !== null)
    footerParts.push(formatUpdatedAt(callsMeta.updatedAt));
  footerParts.push(
    `Showing ${calls.length} calls${cursor !== 0 ? ` from ${cursor + 1}` : ""}${statusFilter !== "" ? ` | ${statusFilter}` : ""}${search !== "" ? ` | "${search}"` : ""}`,
  );

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        {callsMeta.error !== null && callsMeta.error !== "" && (
          <Banner
            type="inline"
            header={callsMeta.errorKind === "offline" ? "You're offline" : "Error"}
            description={callsMeta.error}
          />
        )}
        <Section header="Filters" className="wallet-section">
          <Input
            header="Search"
            placeholder="Last 4 or label"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                setCursor(0);
                void loadCalls(0);
              }
            }}
          />
          <Select
            header="Status"
            value={statusFilter}
            onChange={(event) => setStatusFilter(event.target.value)}
          >
            <option value="">All statuses</option>
            <option value="ringing">Ringing</option>
            <option value="in-progress">In progress</option>
            <option value="completed">Completed</option>
            <option value="no-answer">No answer</option>
            <option value="failed">Failed</option>
          </Select>
          <div className="section-actions">
            <Button
              size="s"
              mode="filled"
              onClick={() => {
                setCursor(0);
                void loadCalls(0);
              }}
            >
              Apply
            </Button>
            <Button size="s" mode="plain" onClick={handleClear}>
              Clear
            </Button>
          </div>
        </Section>

        <Section
          header="Call log"
          footer={footerParts.join(" | ")}
          className="wallet-section"
        >
          {callsMeta.loading && calls.length === 0 ? (
            <SkeletonList rows={5} />
          ) : (
            calls.map((call) => (
              <Cell
                key={call.call_sid}
                subtitle={`${call.status ?? "unknown"} • ${call.created_at ?? "-"}`}
                description={call.call_sid}
                onClick={() => navigate(`/calls/${call.call_sid}`)}
              >
                <MaskedPhone value={call.phone_number ?? call.call_sid} />
              </Cell>
            ))
          )}
        </Section>

        <Section className="wallet-section">
          <InlineButtons mode="gray">
            <InlineButtons.Item
              text="Prev"
              disabled={cursor === 0}
              onClick={handlePrev}
            />
            <InlineButtons.Item
              text="Next"
              disabled={nextCursor === null}
              onClick={handleNext}
            />
          </InlineButtons>
        </Section>
      </List>
    </div>
  );
}
