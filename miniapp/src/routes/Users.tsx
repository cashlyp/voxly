import { useCallback, useEffect, useState } from 'react';
import {
  Button,
  Cell,
  InlineButtons,
  Input,
  List,
  Section,
  Select,
} from '@telegram-apps/telegram-ui';
import { apiFetch, createIdempotencyKey } from '../lib/api';
import { confirmAction } from '../lib/ux';

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
        idempotencyKey: createIdempotencyKey(),
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
      await apiFetch(`/webapp/users/${userId}/promote`, {
        method: 'POST',
        idempotencyKey: createIdempotencyKey(),
      });
      await loadUsers();
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (userId: string) => {
    const confirmed = await confirmAction({
      title: 'Remove user?',
      message: 'This user will lose access immediately.',
      confirmText: 'Remove',
      destructive: true,
    });
    if (!confirmed) return;
    setLoading(true);
    try {
      await apiFetch(`/webapp/users/${userId}`, {
        method: 'DELETE',
        idempotencyKey: createIdempotencyKey(),
      });
      await loadUsers();
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
      <Section header="Allowlisted users" className="wallet-section">
        <Input
          header="Telegram user id"
          placeholder="123456789"
          value={newUserId}
          onChange={(event) => setNewUserId(event.target.value)}
        />
        <Select
          header="Role"
          value={role}
          onChange={(event) => setRole(event.target.value as 'viewer' | 'admin')}
        >
          <option value="viewer">Viewer</option>
          <option value="admin">Admin</option>
        </Select>
        <div className="section-actions">
          <Button size="s" mode="filled" disabled={loading} onClick={handleAdd}>
            Add user
          </Button>
        </div>
      </Section>

      <Section header="Admins" className="wallet-section">
        {admins.map((id) => (
          <Cell
            key={id}
            subtitle="Admin"
            after={(
              <Button size="s" mode="outline" disabled={loading} onClick={() => handleRemove(id)}>
                Remove
              </Button>
            )}
          >
            {id}
          </Cell>
        ))}
      </Section>

      <Section header="Viewers" className="wallet-section">
        {viewers.map((id) => (
          <Cell
            key={id}
            subtitle="Viewer"
            after={(
              <InlineButtons mode="bezeled">
                <InlineButtons.Item text="Promote" disabled={loading} onClick={() => handlePromote(id)} />
                <InlineButtons.Item text="Remove" disabled={loading} onClick={() => handleRemove(id)} />
              </InlineButtons>
            )}
          >
            {id}
          </Cell>
        ))}
      </Section>
      </List>
    </div>
  );
}
