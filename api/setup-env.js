#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const SECTIONS = [
  {
    comments: [
      '# --- API Environment --------------------------',
      '# Primary call provider (twilio | aws | vonage)',
    ],
    fields: [
      { key: 'CALL_PROVIDER', prompt: 'Primary call provider', defaultValue: 'twilio' },
    ],
  },
  {
    comments: ['# API secret (shared for admin + HMAC signing)'],
    fields: [
      { key: 'API_SECRET', prompt: 'API Secret', defaultValue: 'change-me' },
      { key: 'API_HMAC_MAX_SKEW_MS', prompt: 'API HMAC max skew (ms)', defaultValue: '300000' },
    ],
  },
  {
    comments: ['# Twilio credentials (required when CALL_PROVIDER=twilio)'],
    fields: [
      { key: 'TWILIO_ACCOUNT_SID', prompt: 'Twilio Account SID' },
      { key: 'TWILIO_AUTH_TOKEN', prompt: 'Twilio Auth Token' },
      { key: 'FROM_NUMBER', prompt: 'Twilio From Number (E.164 format)' },
    ],
  },
  {
    comments: ['# AWS Connect stack (required when CALL_PROVIDER=aws)'],
    fields: [
      { key: 'AWS_REGION', prompt: 'AWS Region', defaultValue: 'us-east-1' },
      { key: 'AWS_CONNECT_INSTANCE_ID', prompt: 'AWS Connect Instance ID' },
      { key: 'AWS_CONNECT_CONTACT_FLOW_ID', prompt: 'AWS Connect Contact Flow ID' },
      { key: 'AWS_CONNECT_QUEUE_ID', prompt: 'AWS Connect Queue ID' },
      { key: 'AWS_CONNECT_SOURCE_PHONE_NUMBER', prompt: 'AWS Connect Source Phone Number (E.164)' },
      { key: 'AWS_TRANSCRIPTS_QUEUE_URL', prompt: 'AWS Transcripts Queue URL' },
      { key: 'AWS_EVENT_BUS_NAME', prompt: 'AWS EventBridge Bus Name' },
    ],
  },
  {
    comments: ['# AWS media + speech'],
    fields: [
      { key: 'AWS_POLLY_VOICE_ID', prompt: 'AWS Polly Voice ID', defaultValue: 'Joanna' },
      { key: 'AWS_POLLY_OUTPUT_BUCKET', prompt: 'AWS Polly Output Bucket' },
      { key: 'AWS_POLLY_OUTPUT_PREFIX', prompt: 'AWS Polly Output Prefix', defaultValue: 'tts/' },
      { key: 'AWS_MEDIA_BUCKET', prompt: 'AWS Media Bucket (recordings)' },
      { key: 'AWS_PINPOINT_APPLICATION_ID', prompt: 'AWS Pinpoint Application ID' },
      { key: 'AWS_PINPOINT_ORIGINATION_NUMBER', prompt: 'AWS Pinpoint Origination Number (E.164)' },
      { key: 'AWS_PINPOINT_REGION', prompt: 'AWS Pinpoint Region', defaultValue: 'us-east-1' },
      { key: 'AWS_TRANSCRIBE_LANGUAGE_CODE', prompt: 'AWS Transcribe Language Code', defaultValue: 'en-US' },
      { key: 'AWS_TRANSCRIBE_VOCABULARY_FILTER_NAME', prompt: 'AWS Transcribe Vocabulary Filter Name' },
    ],
  },
  {
    comments: ['# Vonage Voice/SMS (required when CALL_PROVIDER=vonage)', '# Provide either the PEM contents (use \\n escapes) or a path to the private key file'],
    fields: [
      { key: 'VONAGE_API_KEY', prompt: 'Vonage API Key' },
      { key: 'VONAGE_API_SECRET', prompt: 'Vonage API Secret' },
      { key: 'VONAGE_APPLICATION_ID', prompt: 'Vonage Application ID' },
      { key: 'VONAGE_PRIVATE_KEY', prompt: 'Vonage Private Key (PEM contents or file path)' },
      { key: 'VONAGE_VOICE_FROM_NUMBER', prompt: 'Vonage Voice From Number (E.164)' },
      { key: 'VONAGE_SMS_FROM_NUMBER', prompt: 'Vonage SMS From Number (E.164)' },
      { key: 'VONAGE_ANSWER_URL', prompt: 'Vonage Answer URL (optional)' },
      { key: 'VONAGE_EVENT_URL', prompt: 'Vonage Event URL (optional)' },
    ],
  },
  {
    comments: ['# Server configuration'],
    fields: [
      { key: 'PORT', prompt: 'API Port', defaultValue: '3000' },
      { key: 'SERVER', prompt: 'Public server hostname (optional)' },
      { key: 'CORS_ORIGINS', prompt: 'Comma-separated CORS origins (optional)' },
    ],
  },
  {
    comments: ['# OpenRouter AI configuration'],
    fields: [
      { key: 'OPENROUTER_API_KEY', prompt: 'OpenRouter API Key' },
      { key: 'OPENROUTER_MODEL', prompt: 'Default OpenRouter Model', defaultValue: 'meta-llama/llama-3.1-8b-instruct:free' },
      { key: 'YOUR_SITE_URL', prompt: 'Your site URL', defaultValue: 'http://localhost:3000' },
      { key: 'YOUR_SITE_NAME', prompt: 'Your site name', defaultValue: 'Voice Call Bot' },
    ],
  },
  {
    comments: ['# Deepgram configuration'],
    fields: [
      { key: 'DEEPGRAM_API_KEY', prompt: 'Deepgram API Key' },
      { key: 'VOICE_MODEL', prompt: 'Deepgram Voice Model', defaultValue: 'aura-asteria-en' },
    ],
  },
  {
    comments: ['# Telegram webhook fallback'],
    fields: [
      { key: 'TELEGRAM_BOT_TOKEN', prompt: 'Telegram Bot Token (optional fallback)' },
    ],
  },
  {
    comments: ['# Call recording'],
    fields: [
      { key: 'RECORDING_ENABLED', prompt: 'Enable call recording? (true/false)', defaultValue: 'false' },
    ],
  },
];

async function confirmOverwrite(targetPath) {
  if (!fs.existsSync(targetPath)) {
    return true;
  }
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const answer = await new Promise((resolve) => rl.question(`⚠️ ${targetPath} already exists. Overwrite? (y/N) `, resolve));
  rl.close();
  return /^y(es)?$/i.test((answer || '').trim());
}

async function collectInputs() {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const answers = {};

  console.log('Provide values for each variable. Press Enter to accept the default or leave blank.');

  const ask = (field) => {
    const defaultLabel = field.defaultValue ? ` [${field.defaultValue}]` : '';
    const question = `${field.prompt}${defaultLabel}: `;
    return new Promise((resolve) => {
      rl.question(question, (input) => {
        const value = input.trim();
        if (value) {
          resolve(value);
        } else if (field.defaultValue !== undefined) {
          resolve(field.defaultValue);
        } else {
          resolve('');
        }
      });
    });
  };

  for (const section of SECTIONS) {
    for (const field of section.fields) {
      // eslint-disable-next-line no-await-in-loop
      answers[field.key] = await ask(field);
    }
  }

  rl.close();
  return answers;
}

function buildEnvContent(values) {
  const lines = [];
  SECTIONS.forEach((section, index) => {
    if (index > 0) {
      lines.push('');
    }
    lines.push(...section.comments);
    section.fields.forEach((field) => {
      lines.push(`${field.key}=${values[field.key] ?? ''}`);
    });
  });
  lines.push('');
  return lines.join('\n');
}

async function main() {
  const targetPath = path.resolve(__dirname, '.env');
  await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });

  if (!(await confirmOverwrite(targetPath))) {
    console.log('Skipping overwrite.');
    return;
  }

  const values = await collectInputs();
  const content = buildEnvContent(values);
  await fs.promises.writeFile(targetPath, content, 'utf8');
  console.log(`✅ Created ${targetPath}`);
  console.log('   Update any remaining blanks before starting the API.');
}

main().catch((error) => {
  console.error('❌ Failed to scaffold API .env file:', error.message);
  process.exit(1);
});
