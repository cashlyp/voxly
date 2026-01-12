'use strict';

/*
 * Configuration for the Telegram bot
 */

require('dotenv').config();
const required = ['ADMIN_TELEGRAM_ID', 'ADMIN_TELEGRAM_USERNAME', 'API_URL', 'BOT_TOKEN', 'ADMIN_API_TOKEN', 'API_HMAC_SECRET'];

const missing = required.filter((key) => !process.env[key]);
if (missing.length > 0) {
  console.error('❌ Bot environment is missing required variables:');
  missing.forEach((key) => console.error(`   - ${key}`));
  console.error('Edit bot/.env and supply the values. You can scaffold the file with `npm run setup --prefix bot` from the repo root.');
  process.exit(1);
}

const templatesApiUrl = process.env.TEMPLATES_API_URL || process.env.API_URL;

try {
  // eslint-disable-next-line no-new
  new URL(templatesApiUrl);
} catch (error) {
  console.error(`❌ Invalid TEMPLATES_API_URL: ${templatesApiUrl || 'undefined'} (${error.message})`);
  process.exit(1);
}

// Check for required environment variables

module.exports = {
  admin: {
    userId: process.env.ADMIN_TELEGRAM_ID,
    username: process.env.ADMIN_TELEGRAM_USERNAME,
    apiToken: process.env.ADMIN_API_TOKEN
  },
  apiUrl: process.env.API_URL,
  botToken: process.env.BOT_TOKEN,
  templatesApiUrl,
  defaultVoiceModel: process.env.DEFAULT_VOICE_MODEL || 'aura-asteria-en',
  defaultBusinessId: process.env.DEFAULT_BUSINESS_ID || 'general',
  defaultPurpose: process.env.DEFAULT_CALL_PURPOSE || 'general',
  apiAuth: {
    hmacSecret: process.env.API_HMAC_SECRET,
  },
};
