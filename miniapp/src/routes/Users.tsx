import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../lib/api';

type UsersResponse = {
  ok: boolean;
  admins: string[];
  viewers: string[];
};

export function Users() {
  const [admins, setAdmins] = useState<string[]>([]);
  const [viewers, setViewers] = useState<string[]>([]);
  const [newUserId, setNewUserId] = useState('');
  const [role, setRole] = useState<'viewer' | 'admin'>('viewer');
  const [loading, setLoading] = useState(false);

  const loadUsers = useCallback(async () => {
    const response = await apiFetch<UsersResponse>('/webapp/users');
    setAdmins(response.admins || []);
    setViewers(response.viewers || []);
  }, []);

  useEffect(() => {
    loadUsers();
  }, [loadUsers]);

  const handleAdd = async () => {
    if (!newUserId) return;
    setLoading(true);
    try {
      await apiFetch('/webapp/users', {
        method: 'POST',
        body: { user_id: newUserId, role },
      });
      setNewUserId('');
      await loadUsers();
    } finally {
      setLoading(false);
    }
  };

  const handlePromote = async (userId: string) => {
    setLoading(true);
    try {
      await apiFetch(`/webapp/users/${userId}/promote`, { method: 'POST' });
      await loadUsers();
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (userId: string) => {
    if (!window.confirm('Remove user from allowlist?')) return;
    setLoading(true);
    try {
      await apiFetch(`/webapp/users/${userId}`, { method: 'DELETE' });
      await loadUsers();
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="stack">
      <div className="panel">
        <h2>Allowlisted users</h2>
        <div className="form inline">
          <input
            type="text"
            placeholder="Telegram user id"
            value={newUserId}
            onChange={(event) => setNewUserId(event.target.value)}
          />
          <select value={role} onChange={(event) => setRole(event.target.value as 'viewer' | 'admin')}>
            <option value="viewer">Viewer</option>
            <option value="admin">Admin</option>
          </select>
          <button type="button" className="btn" disabled={loading} onClick={handleAdd}>
            Add
          </button>
        </div>
      </div>

      <div className="panel">
        <h3>Admins</h3>
        <div className="list">
          {admins.map((id) => (
            <div className="list-item" key={id}>
              <div>
                <strong>{id}</strong>
                <p className="muted">Admin</p>
              </div>
              <button type="button" className="btn danger" disabled={loading} onClick={() => handleRemove(id)}>
                Remove
              </button>
            </div>
          ))}
        </div>
      </div>

      <div className="panel">
        <h3>Viewers</h3>
        <div className="list">
          {viewers.map((id) => (
            <div className="list-item" key={id}>
              <div>
                <strong>{id}</strong>
                <p className="muted">Viewer</p>
              </div>
              <div className="actions">
                <button type="button" className="btn" disabled={loading} onClick={() => handlePromote(id)}>
                  Promote
                </button>
                <button type="button" className="btn danger" disabled={loading} onClick={() => handleRemove(id)}>
                  Remove
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
