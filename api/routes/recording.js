require('colors');
const config = require('../config');

async function recordingService(ttsService, callSid) {
  try {
    console.log(`🎤 Recording service called for call: ${callSid}`.cyan);
    
    if (config.recording.enabled) {
      console.log('📹 Recording is enabled, creating recording...'.green);
      
      if (!config.twilio.accountSid || !config.twilio.authToken) {
        throw new Error('Twilio credentials not configured for recording');
      }
      
      const client = require('twilio')(config.twilio.accountSid, config.twilio.authToken);
      
      // Generate the recording message first
      console.log('🎵 Generating recording announcement...'.cyan);
      await ttsService.generate({
        partialResponseIndex: null, 
        partialResponse: 'This call will be recorded.'
      }, 0);
      
      // Wait a moment for the message to be sent
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      console.log('📞 Creating Twilio recording...'.yellow);
      const recording = await client.calls(callSid)
        .recordings
        .create({
          recordingChannels: 'dual'
        });
          
      console.log(`✅ Recording Created: ${recording.sid}`.green);
      return recording;
    } else {
      console.log('📹 Recording is disabled (config.recording.enabled != true)'.yellow);
      return null;
    }
  } catch (err) {
    console.error('❌ Recording service error:', err);
    // Don't throw the error - let the call continue without recording
    return null;
  }
}

module.exports = { recordingService };
