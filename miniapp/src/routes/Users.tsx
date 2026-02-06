import { useCallback, useEffect, useState } from 'react';
import {
  Banner,
  Button,
  Cell,
  InlineButtons,
  Input,
  List,
  Section,
  Select,
} from '@telegram-apps/telegram-ui';
import { apiFetch, createIdempotencyKey } from '../lib/api';
import { confirmAction, hapticSuccess, hapticError } from '../lib/ux';
import { trackEvent } from '../lib/telemetry';
import { useUser } from '../state/user';

type UsersResponse = {
  ok: boolean;
  admins: string[];
  viewers: string[];
};

export function Users() {
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');
  
  const [admins, setAdmins] = useState<string[]>([]);
  const [viewers, setViewers] = useState<string[]>([]);
  const [newUserId, setNewUserId] = useState('');
  const [role, setRole] = useState<'viewer' | 'admin'>('viewer');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const loadUsers = useCallback(async () => {
    try {
      const response = await apiFetch<UsersResponse>('/webapp/users');
      setAdmins(response.admins || []);
      setViewers(response.viewers || []);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load users');
    }
  }, []);

  useEffect(() => {
    loadUsers();
  }, [loadUsers]);

  const clearMessages = useCallback(() => {
    setError(null);
    setSuccess(null);
  }, []);

  const handleAdd = async () => {
    if (!newUserId.trim()) {
      setError('Please enter a user ID');
      return;
    }
    setLoading(true);
    clearMessages();
    try {
      await apiFetch('/webapp/users', {
        method: 'POST',
        body: { user_id: newUserId.trim(), role },
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      setSuccess(`User ${newUserId} added as ${role}`);
      setNewUserId('');
      trackEvent('user_added', { role });
      await loadUsers();
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : 'Failed to add user');
      trackEvent('user_add_failed', { role });
    } finally {
      setLoading(false);
    }
  };

  const handlePromote = async (userId: string) => {
    const confirmed = await confirmAction({
      title: 'Promote to Admin?',
      message: `${userId} will gain full administrative access.`,
      confirmText: 'Promote',
      destructive: false,
    });
    if (!confirmed) return;
    
    setLoading(true);
    clearMessages();
    try {
      await apiFetch(`/webapp/users/${userId}/promote`, {
        method: 'POST',
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      setSuccess(`${userId} promoted to admin`);
      trackEvent('user_promoted', { user_id: userId });
      await loadUsers();
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : 'Failed to promote user');
      trackEvent('user_promote_failed', { user_id: userId });
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (userId: string) => {
    const confirmed = await confirmAction({
      title: 'Remove user?',
      message: `${userId} will lose all access immediately.`,
      confirmText: 'Remove',
      destructive: true,
    });
    if (!confirmed) return;
    
    setLoading(true);
    clearMessages();
    try {
      await apiFetch(`/webapp/users/${userId}`, {
        method: 'DELETE',
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      setSuccess(`${userId} removed`);
      trackEvent('user_removed', { user_id: userId });
      await loadUsers();
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : 'Failed to remove user');
      trackEvent('user_remove_failed', { user_id: userId });
    } finally {
      setLoading(false);
    }
  };

  if (!isAdmin) {
    return (
      <div className="wallet-page">
        <Banner type="inline" header="Access denied" description="Only administrators can manage users." />
      </div>
    );
  }

  return (
    <div className="wallet-page">
      {error && (
        <Banner 
          type="inline" 
          header="Error" 
          description={error}
        />
      )}
      {success && (
        <Banner 
          type="inline" 
          header="Success" 
          description={success}
        />
      )}
      
      <List className="wallet-list">
        <Section header="Add new user" className="wallet-section">
          <Input
            header="Telegram user ID"
            placeholder="123456789"
            value={newUserId}
            onChange={(event) => setNewUserId(event.target.value)}
            disabled={loading}
          />
          <Select
            header="Role"
            value={role}
            onChange={(event) => setRole(event.target.value as 'viewer' | 'admin')}
            disabled={loading}
          >
            <option value="viewer">👁️ Viewer (read-only)</option>
            <option value="admin">🔧 Admin (full access)</option>
          </Select>
          <div className="section-actions">
            <Button 
              size="s" 
              mode="filled" 
              disabled={loading || !newUserId.trim()}
              onClick={handleAdd}
            >
              Add user
            </Button>
          </div>
        </Section>

        {admins.length > 0 && (
          <Section header={`Administrators (${admins.length})`} className="wallet-section">
            {admins.map((id) => (
              <Cell
                key={id}
                subtitle="🔧 Full access"
                after={(
                  <Button 
                    size="s" 
                    mode="outline" 
                    disabled={loading}
                    onClick={() => handleRemove(id)}
                  >
                    Revoke
                  </Button>
                )}
              >
                {id}
              </Cell>
            ))}
          </Section>
        )}

        {viewers.length > 0 && (
          <Section header={`Viewers (${viewers.length})`} className="wallet-section">
            {viewers.map((id) => (
              <Cell
                key={id}
                subtitle="👁️ Read-only access"
                after={(
                  <InlineButtons mode="bezeled">
                    <InlineButtons.Item 
                      text="Promote" 
                      disabled={loading}
                      onClick={() => handlePromote(id)} 
                    />
                    <InlineButtons.Item 
                      text="Remove" 
                      disabled={loading}
                      onClick={() => handleRemove(id)} 
                    />
                  </InlineButtons>
                )}
              >
                {id}
              </Cell>
            ))}
          </Section>
        )}

        {admins.length === 0 && viewers.length === 0 && (
          <Section className="wallet-section">
            <div className="empty-card">
              <div className="empty-title">No users yet</div>
              <div className="empty-subtitle">Add users above to grant them access</div>
            </div>
          </Section>
        )}
      </List>
    </div>
  );
}
