import { useCallback } from 'react';

type ActivityStatus = 'info' | 'success' | 'error';

type InvokeAction = (
  action: string,
  payload: Record<string, unknown>,
  metaOverride?: Record<string, unknown>,
) => Promise<unknown>;

type RunAction = (
  action: string,
  payload: Record<string, unknown>,
  options?: { confirmText?: string; successMessage?: string },
) => Promise<void>;

type CallScriptsPayload = {
  scripts?: unknown;
  total?: unknown;
  limit?: unknown;
  flow_types?: unknown;
};

type CallScriptSimulationPayload = {
  simulation?: unknown;
};

type UseDashboardCallScriptActionsOptions = {
  invokeAction: InvokeAction;
  runAction: RunAction;
  pushActivity: (status: ActivityStatus, title: string, detail: string) => void;
  setError: (message: string) => void;
  setCallScriptsSnapshot: (payload: CallScriptsPayload) => void;
  setScriptSimulationResult: (payload: CallScriptSimulationPayload | null) => void;
  scriptFlowFilter: string;
  selectedCallScriptId: number;
  scriptNameInput: string;
  scriptDescriptionInput: string;
  scriptDefaultProfileInput: string;
  scriptPromptInput: string;
  scriptFirstMessageInput: string;
  scriptObjectiveTagsInput: string;
  scriptReviewNoteInput: string;
  scriptSimulationVariablesInput: string;
};

type UseDashboardCallScriptActionsResult = {
  refreshCallScriptsModule: () => Promise<void>;
  saveCallScriptDraft: () => Promise<void>;
  submitCallScriptForReview: () => Promise<void>;
  reviewCallScript: (decision: 'approve' | 'reject') => Promise<void>;
  promoteCallScriptLive: () => Promise<void>;
  simulateCallScript: () => Promise<void>;
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function useDashboardCallScriptActions({
  invokeAction,
  runAction,
  pushActivity,
  setError,
  setCallScriptsSnapshot,
  setScriptSimulationResult,
  scriptFlowFilter,
  selectedCallScriptId,
  scriptNameInput,
  scriptDescriptionInput,
  scriptDefaultProfileInput,
  scriptPromptInput,
  scriptFirstMessageInput,
  scriptObjectiveTagsInput,
  scriptReviewNoteInput,
  scriptSimulationVariablesInput,
}: UseDashboardCallScriptActionsOptions): UseDashboardCallScriptActionsResult {
  const refreshCallScriptsModule = useCallback(async (): Promise<void> => {
    try {
      const data = await invokeAction('callscript.list', {
        limit: 120,
        flow_type: scriptFlowFilter || undefined,
      }) as CallScriptsPayload;
      setCallScriptsSnapshot(data);
      pushActivity('success', 'Scripts refreshed', 'Call script list reloaded.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Script refresh failed', detail);
    }
  }, [invokeAction, pushActivity, scriptFlowFilter, setCallScriptsSnapshot, setError]);

  const saveCallScriptDraft = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before saving draft changes.');
      return;
    }
    const payload = {
      id: selectedCallScriptId,
      name: scriptNameInput.trim(),
      description: scriptDescriptionInput,
      default_profile: scriptDefaultProfileInput.trim().toLowerCase(),
      prompt: scriptPromptInput,
      first_message: scriptFirstMessageInput,
      objective_tags: scriptObjectiveTagsInput,
    };
    await runAction(
      'callscript.update',
      payload,
      {
        confirmText: `Save draft changes to script #${selectedCallScriptId}?`,
        successMessage: `Draft updated for script #${selectedCallScriptId}.`,
      },
    );
    await refreshCallScriptsModule();
  }, [
    refreshCallScriptsModule,
    runAction,
    scriptDefaultProfileInput,
    scriptDescriptionInput,
    scriptFirstMessageInput,
    scriptNameInput,
    scriptObjectiveTagsInput,
    scriptPromptInput,
    selectedCallScriptId,
    setError,
  ]);

  const submitCallScriptForReview = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before submitting for review.');
      return;
    }
    await runAction(
      'callscript.submit_review',
      { id: selectedCallScriptId },
      {
        confirmText: `Submit script #${selectedCallScriptId} for review?`,
        successMessage: `Script #${selectedCallScriptId} submitted for review.`,
      },
    );
    await refreshCallScriptsModule();
  }, [refreshCallScriptsModule, runAction, selectedCallScriptId, setError]);

  const reviewCallScript = useCallback(async (decision: 'approve' | 'reject'): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before submitting a review decision.');
      return;
    }
    await runAction(
      'callscript.review',
      {
        id: selectedCallScriptId,
        decision,
        note: scriptReviewNoteInput.trim() || undefined,
      },
      {
        confirmText: `${decision === 'approve' ? 'Approve' : 'Reject'} script #${selectedCallScriptId}?`,
        successMessage: `Review decision recorded for script #${selectedCallScriptId}.`,
      },
    );
    await refreshCallScriptsModule();
  }, [refreshCallScriptsModule, runAction, scriptReviewNoteInput, selectedCallScriptId, setError]);

  const promoteCallScriptLive = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before promoting to live.');
      return;
    }
    await runAction(
      'callscript.promote_live',
      { id: selectedCallScriptId },
      {
        confirmText: `Promote script #${selectedCallScriptId} to live?`,
        successMessage: `Script #${selectedCallScriptId} promoted to live.`,
      },
    );
    await refreshCallScriptsModule();
  }, [refreshCallScriptsModule, runAction, selectedCallScriptId, setError]);

  const simulateCallScript = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before running simulation.');
      return;
    }
    let variables: Record<string, unknown> = {};
    if (scriptSimulationVariablesInput.trim()) {
      try {
        const parsed: unknown = JSON.parse(scriptSimulationVariablesInput);
        variables = asRecord(parsed);
      } catch {
        setError('Simulation variables must be valid JSON.');
        return;
      }
    }
    setError('');
    try {
      const data = await invokeAction('callscript.simulate', {
        id: selectedCallScriptId,
        variables,
      }) as CallScriptSimulationPayload;
      setScriptSimulationResult(data);
      pushActivity('success', 'Script simulation complete', `Simulation ready for script #${selectedCallScriptId}.`);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Script simulation failed', detail);
    }
  }, [invokeAction, pushActivity, scriptSimulationVariablesInput, selectedCallScriptId, setError, setScriptSimulationResult]);

  return {
    refreshCallScriptsModule,
    saveCallScriptDraft,
    submitCallScriptForReview,
    reviewCallScript,
    promoteCallScriptLive,
    simulateCallScript,
  };
}
