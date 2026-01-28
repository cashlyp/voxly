type CallsFilters = {
  status?: string;
  query?: string;
  cursor?: number;
};

export type UiState = {
  path?: string;
  activeCallSid?: string | null;
  callsFilters?: CallsFilters;
};

const STORAGE_KEY = 'voicednut.ui.state';

function readState(): UiState {
  if (typeof sessionStorage === 'undefined') return {};
  const raw = sessionStorage.getItem(STORAGE_KEY);
  if (!raw) return {};
  try {
    return JSON.parse(raw) as UiState;
  } catch {
    return {};
  }
}

function writeState(state: UiState) {
  if (typeof sessionStorage === 'undefined') return;
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

export function loadUiState(): UiState {
  return readState();
}

export function saveUiState(partial: UiState) {
  const current = readState();
  writeState({ ...current, ...partial });
}

export function updateCallsFilters(filters: CallsFilters) {
  const current = readState();
  writeState({
    ...current,
    callsFilters: {
      ...(current.callsFilters || {}),
      ...filters,
    },
  });
}
