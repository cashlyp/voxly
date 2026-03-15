import type { CallScriptRow, DashboardVm } from './types';

type ScriptStudioPageProps = {
  visible: boolean;
  vm: DashboardVm;
};

export function ScriptStudioPage({ visible, vm }: ScriptStudioPageProps) {
  if (!visible) return null;

  const {
    scriptFlowFilter,
    setScriptFlowFilter,
    refreshCallScriptsModule,
    callScriptsTotal,
    callScripts,
    toInt,
    toText,
    selectedCallScriptId,
    setSelectedCallScriptId,
    selectedCallScript,
    selectedCallScriptLifecycleState,
    selectedCallScriptLifecycle,
    formatTime,
    scriptNameInput,
    setScriptNameInput,
    scriptDefaultProfileInput,
    setScriptDefaultProfileInput,
    scriptDescriptionInput,
    setScriptDescriptionInput,
    scriptPromptInput,
    setScriptPromptInput,
    scriptFirstMessageInput,
    setScriptFirstMessageInput,
    scriptObjectiveTagsInput,
    setScriptObjectiveTagsInput,
    busyAction,
    saveCallScriptDraft,
    submitCallScriptForReview,
    scriptReviewNoteInput,
    setScriptReviewNoteInput,
    reviewCallScript,
    promoteCallScriptLive,
    scriptSimulationVariablesInput,
    setScriptSimulationVariablesInput,
    simulateCallScript,
    scriptSimulationResult,
    asRecord,
  } = vm;

  return (
    <section className="va-grid">
      <div className="va-card">
        <h3>Script & Persona Studio</h3>
        <p className="va-muted">
          Draft edits, persona/profile tuning, review approvals, and promote-live workflow.
        </p>
        <div className="va-inline-tools">
          <input
            className="va-input"
            placeholder="Flow filter (optional)"
            value={scriptFlowFilter}
            onChange={(event) => setScriptFlowFilter(event.target.value)}
          />
          <button
            type="button"
            onClick={() => { void refreshCallScriptsModule(); }}
          >
            Refresh Scripts
          </button>
        </div>
        <p className="va-muted">
          Scripts available: <strong>{callScriptsTotal}</strong>
        </p>
        <ul className="va-list">
          {callScripts.slice(0, 60).map((script: CallScriptRow) => {
            const scriptId = toInt(script.id);
            const lifecycle = toText(script.lifecycle_state, 'draft');
            const active = scriptId === selectedCallScriptId;
            return (
              <li key={`call-script-${scriptId}`}>
                <strong>{toText(script.name, `script-${scriptId || 'unknown'}`)}</strong>
                <span>
                  ID: {scriptId} | Flow: {toText(script.flow_type, 'general')} | v{toInt(script.version, 1)}
                </span>
                <span>
                  Lifecycle: <strong>{lifecycle}</strong>
                  {' '}| Persona(default_profile): <strong>{toText(script.default_profile, 'general')}</strong>
                </span>
                <button
                  type="button"
                  className={active ? 'va-chip is-active' : 'va-chip'}
                  onClick={() => setSelectedCallScriptId(scriptId)}
                >
                  {active ? 'Selected' : 'Select'}
                </button>
              </li>
            );
          })}
        </ul>
      </div>

      <div className="va-card">
        <h3>Draft Editor & Approval</h3>
        {selectedCallScript ? (
          <>
            <p>
              Editing script: <strong>{toText(selectedCallScript.name, 'unknown')}</strong>
              {' '}(# {selectedCallScriptId})
            </p>
            <p className="va-muted">
              Lifecycle: <strong>{selectedCallScriptLifecycleState}</strong>
              {' '}| Submitted: <strong>{formatTime(selectedCallScriptLifecycle.submitted_for_review_at)}</strong>
              {' '}| Reviewed: <strong>{formatTime(selectedCallScriptLifecycle.reviewed_at)}</strong>
            </p>
            <input
              className="va-input"
              placeholder="Script name"
              value={scriptNameInput}
              onChange={(event) => setScriptNameInput(event.target.value)}
            />
            <input
              className="va-input"
              placeholder="Persona profile (default_profile)"
              value={scriptDefaultProfileInput}
              onChange={(event) => setScriptDefaultProfileInput(event.target.value)}
            />
            <textarea
              className="va-input va-textarea"
              placeholder="Description"
              value={scriptDescriptionInput}
              onChange={(event) => setScriptDescriptionInput(event.target.value)}
              rows={2}
            />
            <textarea
              className="va-input va-textarea"
              placeholder="Prompt"
              value={scriptPromptInput}
              onChange={(event) => setScriptPromptInput(event.target.value)}
              rows={6}
            />
            <textarea
              className="va-input va-textarea"
              placeholder="First message"
              value={scriptFirstMessageInput}
              onChange={(event) => setScriptFirstMessageInput(event.target.value)}
              rows={3}
            />
            <input
              className="va-input"
              placeholder="Objective tags (comma-separated)"
              value={scriptObjectiveTagsInput}
              onChange={(event) => setScriptObjectiveTagsInput(event.target.value)}
            />
            <div className="va-inline-tools">
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void saveCallScriptDraft(); }}
              >
                Save Draft
              </button>
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void submitCallScriptForReview(); }}
              >
                Submit Review
              </button>
            </div>
            <textarea
              className="va-input va-textarea"
              placeholder="Review note / approval reason"
              value={scriptReviewNoteInput}
              onChange={(event) => setScriptReviewNoteInput(event.target.value)}
              rows={2}
            />
            <div className="va-inline-tools">
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void reviewCallScript('approve'); }}
              >
                Approve
              </button>
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void reviewCallScript('reject'); }}
              >
                Reject
              </button>
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void promoteCallScriptLive(); }}
              >
                Promote Live
              </button>
            </div>
            <h3>Simulation</h3>
            <textarea
              className="va-input va-textarea"
              placeholder={'Variables JSON, e.g. {"customer_name":"Ada"}'}
              value={scriptSimulationVariablesInput}
              onChange={(event) => setScriptSimulationVariablesInput(event.target.value)}
              rows={3}
            />
            <div className="va-inline-tools">
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void simulateCallScript(); }}
              >
                Run Simulation
              </button>
            </div>
            {scriptSimulationResult ? (
              <pre>
                {JSON.stringify(asRecord(scriptSimulationResult).simulation || scriptSimulationResult, null, 2)}
              </pre>
            ) : null}
          </>
        ) : (
          <p className="va-muted">Select a script from the list to edit and review.</p>
        )}
      </div>
    </section>
  );
}
