import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../lib/api';
import { useUser } from '../state/user';

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
  name: '',
  description: '',
  prompt: '',
  first_message: '',
  business_id: '',
  voice_model: '',
};

export function Scripts() {
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');
  const [scripts, setScripts] = useState<Script[]>([]);
  const [selected, setSelected] = useState<Script | null>(null);
  const [draft, setDraft] = useState<Partial<Script>>(emptyDraft);
  const [saving, setSaving] = useState(false);

  const loadScripts = useCallback(async () => {
    const response = await apiFetch<{ ok: boolean; scripts: Script[] }>('/webapp/scripts');
    const nextScripts = response.scripts || [];
    setScripts(nextScripts);
    const fallback = nextScripts[0] || null;
    setSelected((prev) => prev || fallback);
    setDraft((prev) => (prev && prev.id ? prev : (fallback || emptyDraft)));
  }, []);

  useEffect(() => {
    loadScripts();
  }, [loadScripts]);

  const hasSelection = useMemo(() => Boolean(selected?.id), [selected]);

  const handleSave = async () => {
    if (!isAdmin) return;
    setSaving(true);
    try {
      if (hasSelection && selected?.id) {
        const response = await apiFetch<{ ok: boolean; script: Script }>(`/webapp/scripts/${selected.id}`, {
          method: 'PUT',
          body: draft,
        });
        setSelected(response.script);
      } else {
        const response = await apiFetch<{ ok: boolean; script: Script }>('/webapp/scripts', {
          method: 'POST',
          body: draft,
        });
        setSelected(response.script);
      }
      await loadScripts();
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!isAdmin || !selected?.id) return;
    if (!window.confirm('Delete this script?')) return;
    setSaving(true);
    try {
      await apiFetch(`/webapp/scripts/${selected.id}`, { method: 'DELETE' });
      setSelected(null);
      setDraft(emptyDraft);
      await loadScripts();
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="grid-two">
      <div className="panel">
        <h2>Script library</h2>
        <div className="list">
          {scripts.map((script) => (
            <button
              key={script.id}
              type="button"
              className={`list-item clickable${selected?.id === script.id ? ' active' : ''}`}
              onClick={() => {
                setSelected(script);
                setDraft(script);
              }}
            >
              <div>
                <strong>{script.name}</strong>
                <p className="muted">{script.description || 'No description'}</p>
              </div>
            </button>
          ))}
        </div>
        {isAdmin && (
          <button
            type="button"
            className="btn"
            onClick={() => {
              setSelected(null);
              setDraft(emptyDraft);
            }}
          >
            New script
          </button>
        )}
      </div>

      <div className="panel">
        <h2>{hasSelection ? 'Edit script' : 'Create script'}</h2>
        <div className="form">
          <label htmlFor="script-name">Name</label>
          <input
            id="script-name"
            type="text"
            value={draft.name || ''}
            onChange={(event) => setDraft((prev) => ({ ...prev, name: event.target.value }))}
          />
          <label htmlFor="script-description">Description</label>
          <input
            id="script-description"
            type="text"
            value={draft.description || ''}
            onChange={(event) => setDraft((prev) => ({ ...prev, description: event.target.value }))}
          />
          <label htmlFor="script-prompt">Prompt</label>
          <textarea
            id="script-prompt"
            rows={4}
            value={draft.prompt || ''}
            onChange={(event) => setDraft((prev) => ({ ...prev, prompt: event.target.value }))}
          />
          <label htmlFor="script-first-message">First message</label>
          <textarea
            id="script-first-message"
            rows={3}
            value={draft.first_message || ''}
            onChange={(event) => setDraft((prev) => ({ ...prev, first_message: event.target.value }))}
          />
          <label htmlFor="script-voice">Voice model</label>
          <input
            id="script-voice"
            type="text"
            value={draft.voice_model || ''}
            onChange={(event) => setDraft((prev) => ({ ...prev, voice_model: event.target.value }))}
          />
        </div>
        <div className="actions">
          <button type="button" className="btn" disabled={!isAdmin || saving} onClick={handleSave}>
            {saving ? 'Saving...' : 'Save'}
          </button>
          {hasSelection && (
            <button type="button" className="btn danger" disabled={!isAdmin || saving} onClick={handleDelete}>
              Delete
            </button>
          )}
        </div>
      </div>
    </section>
  );
}
