const { twilio } = require('../config');

const transferCall = async function (call) {

  console.log('Transferring call', call.callSid);
  const accountSid = twilio.accountSid;
  const authToken = twilio.authToken;
  const client = require('twilio')(accountSid, authToken);
  const targetNumber = twilio.transferNumber;

  return await client.calls(call.callSid)
    .update({twiml: `<Response><Dial>${targetNumber}</Dial></Response>`})
    .then(() => {
      return 'The call was transferred successfully, say goodbye to the customer.';
    })
    .catch(() => {
      return 'The call was not transferred successfully, advise customer to call back later.';
    });
};

module.exports = transferCall;
