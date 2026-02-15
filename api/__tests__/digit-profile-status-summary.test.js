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

  test('sendCallTranscript sends transcript card when audio exists without text transcript', async () => {
    const originalDb = webhookService.db;
    const originalSendTelegramMessage = webhookService.sendTelegramMessage;
    try {
      webhookService.db = {
        getCall: jest.fn().mockResolvedValue({
          call_sid: 'CA_AUDIO_ONLY',
          phone_number: '+16125550100'
        }),
        getCallTranscripts: jest.fn().mockResolvedValue([]),
        getCallStates: jest.fn().mockResolvedValue([
          { data: { audio_url: 'https://example.com/transcript-audio.mp3' } }
        ]),
        logNotificationMetric: jest.fn().mockResolvedValue(true)
      };
      webhookService.sendTelegramMessage = jest.fn().mockResolvedValue(true);

      await webhookService.sendCallTranscript('CA_AUDIO_ONLY', '123');

      expect(webhookService.sendTelegramMessage).toHaveBeenCalledWith(
        '123',
        expect.stringContaining('📋 Transcript ready for'),
        false,
        expect.objectContaining({
          replyMarkup: expect.objectContaining({
            inline_keyboard: expect.any(Array)
          })
        })
      );
    } finally {
      webhookService.db = originalDb;
      webhookService.sendTelegramMessage = originalSendTelegramMessage;
    }
  });
});
