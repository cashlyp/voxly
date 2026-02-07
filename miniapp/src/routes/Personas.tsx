import {
  Badge,
  Banner,
  Button,
  Input,
  List,
  Section,
  Textarea,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useState } from "react";
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { trackEvent } from "../lib/telemetry";
import { confirmAction, hapticError, hapticSuccess } from "../lib/ux";
import { useUser } from "../state/user";

type Persona = {
  id: number;
  name: string;
  description?: string | null;
  system_prompt?: string | null;
  voice_model?: string | null;
  created_at?: string;
  updated_at?: string;
};

const emptyDraft: Partial<Persona> = {
  name: "",
  description: "",
  system_prompt: "",
  voice_model: "",
};

export function Personas() {
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");

  const [personas, setPersonas] = useState<Persona[]>([]);
  const [selected, setSelected] = useState<Persona | null>(null);
  const [draft, setDraft] = useState<Partial<Persona>>(emptyDraft);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);

  const loadPersonas = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiFetch<{ ok: boolean; personas: Persona[] }>(
        "/webapp/personas",
      );
      setPersonas(response.personas || []);
      const fallback = response.personas?.[0] || null;
      setSelected(fallback);
      setDraft(fallback || emptyDraft);
      setIsEditing(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load personas");
      trackEvent("personas_load_failed");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadPersonas();
  }, [loadPersonas]);

  const handleSelect = (persona: Persona) => {
    setSelected(persona);
    setDraft(persona);
    setIsEditing(false);
    setError(null);
    setSuccess(null);
  };

  const handleEdit = () => {
    if (!isAdmin) return;
    setIsEditing(true);
    setError(null);
  };

  const handleCancel = () => {
    setIsEditing(false);
    if (selected !== null && selected !== undefined) {
      setDraft(selected);
    }
  };

  const handleSave = async () => {
    if (!isAdmin) return;

    if (!draft.name?.trim()) {
      setError("Name is required");
      return;
    }

    setSaving(true);
    setError(null);
    setSuccess(null);

    try {
      if (selected?.id !== undefined) {
        // Update existing
        const response = await apiFetch<{ ok: boolean; persona: Persona }>(
          `/webapp/personas/${selected.id}`,
          {
            method: "PUT",
            body: {
              name: draft.name?.trim(),
              description: draft.description?.trim() || null,
              system_prompt: draft.system_prompt?.trim() || null,
              voice_model: draft.voice_model?.trim() || null,
            },
            idempotencyKey: createIdempotencyKey(),
          },
        );

        if (response.ok) {
          hapticSuccess();
          setSuccess("Persona updated successfully!");
          setSelected(response.persona);
          setDraft(response.persona);
          trackEvent("persona_updated", { persona_id: selected.id });
          await loadPersonas();
        } else {
          throw new Error("Failed to update persona");
        }
      } else {
        // Create new
        const response = await apiFetch<{ ok: boolean; persona: Persona }>(
          "/webapp/personas",
          {
            method: "POST",
            body: {
              name: draft.name?.trim(),
              description: draft.description?.trim() || null,
              system_prompt: draft.system_prompt?.trim() || null,
              voice_model: draft.voice_model?.trim() || null,
            },
            idempotencyKey: createIdempotencyKey(),
          },
        );

        if (response.ok) {
          hapticSuccess();
          setSuccess("Persona created successfully!");
          setSelected(response.persona);
          setDraft(response.persona);
          trackEvent("persona_created");
          await loadPersonas();
        } else {
          throw new Error("Failed to create persona");
        }
      }

      setIsEditing(false);
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : "Failed to save persona");
      if (selected?.id) {
        trackEvent("persona_update_failed", { persona_id: selected.id });
      } else {
        trackEvent("persona_create_failed");
      }
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!isAdmin || !selected?.id) return;

    const confirmed = await confirmAction({
      title: "Delete Persona?",
      message: `This will permanently delete "${selected.name}".`,
      confirmText: "Delete",
      destructive: true,
    });

    if (!confirmed) return;

    setSaving(true);
    setError(null);

    try {
      await apiFetch(`/webapp/personas/${selected.id}`, {
        method: "DELETE",
        idempotencyKey: createIdempotencyKey(),
      });

      hapticSuccess();
      setSuccess("Persona deleted successfully!");
      trackEvent("persona_deleted", { persona_id: selected.id });
      await loadPersonas();
      setIsEditing(false);
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : "Failed to delete persona");
      trackEvent("persona_delete_failed", { persona_id: selected.id });
    } finally {
      setSaving(false);
    }
  };

  const handleCreateNew = () => {
    setSelected(null);
    setDraft(emptyDraft);
    setIsEditing(true);
    setError(null);
    setSuccess(null);
  };

  const hasSelection = Boolean(selected?.id);

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        {error && <Banner type="inline" header="Error" description={error} />}
        {success && (
          <Banner type="inline" header="Success" description={success} />
        )}

        <Section header="AI Personas" className="wallet-section">
          <div className="card-header">
            <span>Available Personas</span>
            {isAdmin && (
              <Button size="s" mode="bezeled" onClick={handleCreateNew}>
                New
              </Button>
            )}
          </div>

          {loading ? (
            <div className="empty-card">
              <div className="empty-title">Loading personas...</div>
            </div>
          ) : personas.length === 0 ? (
            <div className="empty-card">
              <div className="empty-title">No personas</div>
              <div className="empty-subtitle">
                {isAdmin
                  ? "Create a new persona to get started."
                  : "No personas available."}
              </div>
            </div>
          ) : (
            <div className="card-list">
              {personas.map((persona) => (
                <button
                  key={persona.id}
                  type="button"
                  className={`card-item card-item-button ${
                    selected?.id === persona.id ? "selected" : ""
                  }`}
                  onClick={() => void handleSelect(persona)}
                >
                  <div className="card-item-main">
                    <div className="card-item-title">{persona.name}</div>
                    <div className="card-item-subtitle">
                      {persona.description || "No description"}
                    </div>
                    {persona.voice_model && (
                      <div className="card-item-meta">
                        Voice: {persona.voice_model}
                      </div>
                    )}
                  </div>
                  {persona.id && <Badge type="dot">Info</Badge>}
                </button>
              ))}
            </div>
          )}
        </Section>

        {(selected || isEditing) && (
          <Section header={isEditing ? "Edit Persona" : "Persona Details"}>
            <Input
              header="Name"
              placeholder="Persona name"
              value={draft.name || ""}
              onChange={(e) =>
                void setDraft({ ...draft, name: e.target.value })
              }
              disabled={!isEditing || saving}
            />

            <Input
              header="Voice Model"
              placeholder="e.g., en-US-Neural2-C"
              value={draft.voice_model || ""}
              onChange={(e) =>
                void setDraft({ ...draft, voice_model: e.target.value })
              }
              disabled={!isEditing || saving}
            />

            <Textarea
              header="Description"
              placeholder="Brief description of this persona..."
              value={draft.description || ""}
              onChange={(e) =>
                void setDraft({ ...draft, description: e.target.value })
              }
              disabled={!isEditing || saving}
            />

            <Textarea
              header="System Prompt"
              placeholder="The instruction prompt that defines this AI persona's behavior..."
              value={draft.system_prompt || ""}
              onChange={(e) =>
                void setDraft({ ...draft, system_prompt: e.target.value })
              }
              disabled={!isEditing || saving}
            />

            {isEditing ? (
              <div className="section-actions">
                <Button
                  size="m"
                  mode="filled"
                  onClick={() => void handleSave()}
                  disabled={saving || !draft.name?.trim()}
                >
                  {saving ? "Saving..." : "Save Changes"}
                </Button>
                <Button
                  size="m"
                  mode="plain"
                  onClick={() => void handleCancel()}
                  disabled={saving}
                >
                  Cancel
                </Button>
                {hasSelection && isAdmin && (
                  <Button
                    size="m"
                    mode="plain"
                    onClick={() => void handleDelete()}
                    disabled={saving}
                    style={{ color: "var(--tg-theme-destructive-text-color)" }}
                  >
                    Delete
                  </Button>
                )}
              </div>
            ) : (
              <div className="section-actions">
                {isAdmin && (
                  <Button
                    size="m"
                    mode="filled"
                    onClick={() => void handleEdit()}
                  >
                    Edit
                  </Button>
                )}
              </div>
            )}
          </Section>
        )}
      </List>
    </div>
  );
}
