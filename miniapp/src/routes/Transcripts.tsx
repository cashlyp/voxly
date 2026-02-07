import { Banner, Cell, List, Placeholder, Section } from "@telegram-apps/telegram-ui";
import { useEffect } from "react";
import { MaskedPhone } from "../components/MaskedPhone";
import { SkeletonList } from "../components/Skeleton";
import { navigate } from "../lib/router";
import { useCalls } from "../state/calls";

export function Transcripts() {
  const { calls, fetchCalls, callsMeta } = useCalls();

  useEffect(() => {
    void fetchCalls({ limit: 20, status: "completed" });
  }, [fetchCalls]);

  return (
    <div className="wallet-page">
      {callsMeta.error !== null && callsMeta.error !== "" && (
        <Banner
          type="inline"
          header={callsMeta.errorKind === "offline" ? "You're offline" : "Error"}
          description={callsMeta.error}
        />
      )}
      <List className="wallet-list">
        <Section
          header="Transcripts"
          footer="Tap a call to view its transcript."
          className="wallet-section"
        >
          {callsMeta.loading && calls.length === 0 ? (
            <SkeletonList rows={6} />
          ) : calls.length === 0 ? (
            <Placeholder
              header="No transcripts yet"
              description="Completed calls will appear here."
            />
          ) : (
            calls.map((call) => (
              <Cell
                key={call.call_sid}
                subtitle={`${call.status ?? "completed"} - ${call.created_at ?? "-"}`}
                description={call.call_sid}
                onClick={() => navigate(`/transcripts/${call.call_sid}`)}
              >
                <MaskedPhone value={call.phone_number ?? call.call_sid} />
              </Cell>
            ))
          )}
        </Section>
      </List>
    </div>
  );
}
