#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const readline = require('readline');

const SECTIONS = [
  {
    comments: ['# --- Telegram Bot Environment -----------------------'],
    fields: [
      { key: 'ADMIN_TELEGRAM_ID', prompt: 'Admin Telegram ID' },
      { key: 'ADMIN_TELEGRAM_USERNAME', prompt: 'Admin Telegram Username (without @)' },
      { key: 'BOT_TOKEN', prompt: 'Telegram Bot Token' },
    ],
  },
  {
    comments: ['# API the bot should call (point to /api server)'],
    fields: [
      { key: 'API_URL', prompt: 'API URL', defaultValue: 'http://localhost:3000' },
      { key: 'TEMPLATES_API_URL', prompt: 'Templates API URL', defaultValue: 'http://localhost:3000' },
    ],
  },
  {
    comments: ['# Bot admin API token (must match API ADMIN_API_TOKEN)'],
    fields: [
      { key: 'ADMIN_API_TOKEN', prompt: 'Admin API Token', defaultValue: 'change-me' },
    ],
  },
  {
    comments: ['# API request signing (must match API_HMAC_SECRET)'],
    fields: [
      { key: 'API_HMAC_SECRET', prompt: 'API HMAC Secret', defaultValue: 'change-me' },
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
  console.log('   Update any remaining blanks before starting the bot.');
}

main().catch((error) => {
  console.error('❌ Failed to scaffold bot .env file:', error.message);
  process.exit(1);
});
