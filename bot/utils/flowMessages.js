const { section } = require('./ui');

function selectionExpiredMessage() {
  return section('⌛ Selection expired', [
    'That menu button is no longer active.',
    'Please choose an option again.'
  ]);
}

function cancelledMessage(flowLabel = 'Action', nextHint = 'Use /menu to continue.') {
  return section(`🛑 ${flowLabel} cancelled`, [
    'No changes were applied.',
    nextHint
  ]);
}

function setupStepMessage(flowLabel, lines = []) {
  return section(`🧭 ${flowLabel}`, lines);
}

module.exports = {
  selectionExpiredMessage,
  cancelledMessage,
  setupStepMessage
};
