import {
  Badge,
  Button,
  Cell,
  Input,
  List,
  Section,
  Textarea,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useMemo, useState } from "react";
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { confirmAction } from "../lib/ux";
import { useUser } from "../state/user";

type Script = {
  id: number;
  name: string;
  description?: string | null;
  prompt?: string | null;
  first_message?: string | null;
  business_id?: string | null;
  voice_model?: string | null;
};

const emptyDraft: Partial<Script> = {
  name: "",
  description: "",
  prompt: "",
  first_message: "",
  business_id: "",
  voice_model: "",
};

export function Scripts() {
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");
  const [scripts, setScripts] = useState<Script[]>([]);
  const [selected, setSelected] = useState<Script | null>(null);
  const [draft, setDraft] = useState<Partial<Script>>(emptyDraft);
  const [saving, setSaving] = useState(false);

  const loadScripts = useCallback(async () => {
    const response = await apiFetch<{ ok: boolean; scripts: Script[] }>(
      "/webapp/scripts",
    );
    const nextScripts = response.scripts || [];
    setScripts(nextScripts);
    const fallback = nextScripts[0] || null;
    setSelected((prev) => prev || fallback);
    setDraft((prev) => (prev && prev.id ? prev : fallback || emptyDraft));
  }, []);

  useEffect(() => {
    loadScripts();
  }, [loadScripts]);

  const hasSelection = useMemo(() => Boolean(selected?.id), [selected]);

  const handleSave = async () => {
    if (!isAdmin) return;
    setSaving(true);
    try {
      if (selected?.id !== undefined) {
        const response = await apiFetch<{ ok: boolean; script: Script }>(
          `/webapp/scripts/${selected.id}`,
          {
            method: "PUT",
            body: draft,
            idempotencyKey: createIdempotencyKey(),
          },
        );
        setSelected(response.script);
      } else {
        const response = await apiFetch<{ ok: boolean; script: Script }>(
          "/webapp/scripts",
          {
            method: "POST",
            body: draft,
            idempotencyKey: createIdempotencyKey(),
          },
        );
        setSelected(response.script);
      }
      await loadScripts();
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!isAdmin || !selected?.id) return;
    const confirmed = await confirmAction({
      title: "Delete this script?",
      message: "This cannot be undone.",
      confirmText: "Delete",
      destructive: true,
    });
    if (!confirmed) return;
    setSaving(true);
    try {
      await apiFetch(`/webapp/scripts/${selected.id}`, {
        method: "DELETE",
        idempotencyKey: createIdempotencyKey(),
      });
      setSelected(null);
      setDraft(emptyDraft);
      await loadScripts();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        <Section header="Script library" className="wallet-section">
          {scripts.map((script) => (
            <Cell
              key={script.id}
              subtitle={script.description || "No description"}
              titleBadge={
                selected?.id === script.id ? (
                  <Badge type="dot" mode="primary" />
                ) : undefined
              }
              onClick={() => {
                setSelected(script);
                setDraft(script);
              }}
            >
              {script.name}
            </Cell>
          ))}
          {isAdmin && (
            <div className="section-actions">
              <Button
                size="s"
                mode="bezeled"
                onClick={() => {
                  void setSelected(null);
                  void setDraft(emptyDraft);
                }}
              >
                New script
              </Button>
            </div>
          )}
        </Section>

        <Section
          header={hasSelection ? "Edit script" : "Create script"}
          className="wallet-section"
        >
          <Input
            header="Name"
            value={draft.name || ""}
            onChange={(event) =>
              void setDraft((prev) => ({ ...prev, name: event.target.value }))
            }
          />
          <Input
            header="Description"
            value={draft.description || ""}
            onChange={(event) =>
              void setDraft((prev) => ({
                ...prev,
                description: event.target.value,
              }))
            }
          />
          <Textarea
            header="Prompt"
            rows={4}
            value={draft.prompt || ""}
            onChange={(event) =>
              void setDraft((prev) => ({ ...prev, prompt: event.target.value }))
            }
          />
          <Textarea
            header="First message"
            rows={3}
            value={draft.first_message || ""}
            onChange={(event) =>
              void setDraft((prev) => ({
                ...prev,
                first_message: event.target.value,
              }))
            }
          />
          <Input
            header="Voice model"
            value={draft.voice_model || ""}
            onChange={(event) =>
              void setDraft((prev) => ({
                ...prev,
                voice_model: event.target.value,
              }))
            }
          />
          <div className="section-actions">
            <Button
              size="s"
              mode="filled"
              disabled={!isAdmin || saving}
              onClick={() => void handleSave()}
            >
              {saving ? "Saving..." : "Save"}
            </Button>
            {hasSelection && (
              <Button
                size="s"
                mode="outline"
                disabled={!isAdmin || saving}
                onClick={() => void handleDelete()}
              >
                Delete
              </Button>
            )}
          </div>
        </Section>
      </List>
    </div>
  );
}
