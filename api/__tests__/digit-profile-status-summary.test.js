'use strict';

const { webhookService } = require('../routes/status');

describe('digit profile status summary rendering', () => {
  test('suppresses generic Digits line when OTP profile is present', () => {
    const summary = webhookService.buildDigitSummaryFromEvents([
      { profile: 'otp', accepted: false, digits: '' },
      { profile: 'generic', accepted: false, digits: '' }
    ]);

    expect(summary).toContain('OTP');
    expect(summary).toContain('OTP: none (unverified)');
    expect(summary).not.toContain('Digits:');
  });

  test('keeps generic Digits line when no specific profile is present', () => {
    const summary = webhookService.buildDigitSummaryFromEvents([
      { profile: 'generic', accepted: false, digits: '' }
    ]);

    expect(summary).toContain('Digits: none (unverified)');
  });
});
