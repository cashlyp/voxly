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
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';

export function Calls() {
  const { calls, fetchCalls, nextCursor, loading } = useCalls();
  const [statusFilter, setStatusFilter] = useState('');
  const [search, setSearch] = useState('');
  const [cursor, setCursor] = useState(0);

  const loadCalls = useCallback(async (nextCursorValue = 0) => {
    await fetchCalls({
      limit: 20,
      cursor: nextCursorValue,
      status: statusFilter || undefined,
      q: search || undefined,
    });
  }, [fetchCalls, statusFilter, search]);

  useEffect(() => {
    loadCalls(0);
  }, [loadCalls]);

  const handleNext = () => {
    if (nextCursor !== null) {
      setCursor(nextCursor);
      loadCalls(nextCursor);
    }
  };

  const handlePrev = () => {
    const prev = Math.max(0, cursor - 20);
    setCursor(prev);
    loadCalls(prev);
  };

  const handleClear = () => {
    setStatusFilter('');
    setSearch('');
    setCursor(0);
    loadCalls(0);
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
      <Section header="Filters" className="wallet-section">
        <Input
          header="Search"
          placeholder="Last 4 or label"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              setCursor(0);
              loadCalls(0);
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
              loadCalls(0);
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
        footer={`Showing ${calls.length} calls${cursor ? ` from ${cursor + 1}` : ''}${statusFilter ? ` | ${statusFilter}` : ''}${search ? ` | "${search}"` : ''}`}
        className="wallet-section"
      >
        {loading && calls.length === 0 ? (
          <Cell subtitle="Loading calls...">Please wait</Cell>
        ) : (
          calls.map((call) => (
            <Cell
              key={call.call_sid}
              subtitle={`${call.status || 'unknown'} • ${call.created_at || '-'}`}
              description={call.call_sid}
              onClick={() => navigate(`/calls/${call.call_sid}`)}
            >
              {call.phone_number || call.call_sid}
            </Cell>
          ))
        )}
      </Section>

      <Section className="wallet-section">
        <InlineButtons mode="gray">
          <InlineButtons.Item text="Prev" disabled={cursor === 0} onClick={handlePrev} />
          <InlineButtons.Item text="Next" disabled={nextCursor === null} onClick={handleNext} />
        </InlineButtons>
      </Section>
      </List>
    </div>
  );
}
