const { twilio } = require('../config');
const { runWithTimeout } = require('../utils/asyncControl');

const transferCall = async function (call) {
  console.log('Transferring call', call.callSid);
  const accountSid = twilio.accountSid;
  const authToken = twilio.authToken;
  const client = require('twilio')(accountSid, authToken);
  const targetNumber = twilio.transferNumber;
  const timeoutMs =
    Number(twilio?.requestTimeoutMs) ||
    Number(process.env.TWILIO_REQUEST_TIMEOUT_MS) ||
    15000;

  try {
    await runWithTimeout(
      client.calls(call.callSid).update({
        twiml: `<Response><Dial>${targetNumber}</Dial></Response>`,
      }),
      {
        timeoutMs,
        label: 'transfer_call_update',
        timeoutCode: 'transfer_call_timeout',
        logger: console,
        meta: {
          scope: 'transfer_call',
        },
        warnAfterMs: Math.max(1000, Math.min(5000, Math.floor(timeoutMs / 2))),
      },
    );
    return 'The call was transferred successfully, say goodbye to the customer.';
  } catch (_error) {
    return 'The call was not transferred successfully, advise the customer to call back later.';
  }
};

module.exports = transferCall;
