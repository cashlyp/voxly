# Voice Call Bot System 🤖📞

[![Node.js](https://img.shields.io/badge/Node.js-16%2B-green.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-blue.svg)](https://core.telegram.org/bots)
[![Twilio](https://img.shields.io/badge/Twilio-API-red.svg)](https://www.twilio.com/)

A comprehensive Telegram bot system for making AI-powered voice calls using Twilio, OpenRouter APIs, and real-time transcription with Deepgram.

### 🌟 Features

### 🤖 Bot Features

- AI-powered voice conversations with customizable agents
- Outbound call management and monitoring
- Real-time transcription and conversation logging
- Call analytics, summaries, and detailed reporting
- Multi-user authorization system with role-based access
- Admin controls for user and system management
- Complete call history with searchable transcripts
- Real-time notifications via Telegram webhooks
- Custom AI prompts and agent personalities

### 🌐 API Features

- RESTful API for call management
- Real-time call status and progress tracking
- WebSocket connections for live audio streaming
- Persistent data storage with SQLite
- Secure webhook notifications
- Comprehensive health monitoring
- Text-to-speech with Deepgram
- Speech-to-text with real-time processing

## 🔁 Provider Modes (Twilio, AWS Connect, Vonage)

- Set the default voice backbone with `CALL_PROVIDER` in `api/.env` (`twilio`, `aws`, or `vonage`).
- Admins can switch live traffic at any time via `/provider <name>|status` in Telegram (requires `ADMIN_API_TOKEN`).
- Twilio mode keeps the original Media Streams flow; AWS mode routes through Connect/Kinesis per `aws-migration.md`; Vonage mode uses the Vonage Voice API and WebSocket streaming.
- Each provider has matching SMS adapters (Twilio SMS, AWS Pinpoint, Vonage SMS) so outbound texts follow the active backbone.
- When Twilio is selected but credentials are missing, the API now fails fast with a targeted message pointing to the relevant `.env` entries and the `npm run setup --prefix api` helper.

## 🏗️ System Architecture

```mermaid
graph TB
    A[Telegram Bot<br/>Grammy.js] --> B[API Server<br/>Express.js]
    B --> C[Twilio API<br/>Voice Calls]
    A --> D[SQLite DB<br/>User Auth]
    B --> E[Database<br/>Call Data]
    B --> F[AI Services<br/>OpenRouter]
    B --> G[Deepgram<br/>Transcription]
    
    style A fill:#0088cc
    style B fill:#ff6b35
    style C fill:#f25c54
    style F fill:#4ecdc4
    style G fill:#45b7d1
```

## 🚀 Quick Start

### Prerequisites

- Node.js 16+
- [Telegram Bot Token](https://t.me/botfather)
- [Twilio Account](https://console.twilio.com/) (Account SID, Auth Token, Phone Number)
- [OpenRouter API Key](https://openrouter.ai/)
- [Deepgram API Key](https://deepgram.com/)

### Installation

1. **Clone the repository**
   
   ```bash
   git clone https://github.com/voicednut/voicednut.git
   cd voicednut
   ```
1. **Generate environment files**

```bash
npm run setup --prefix api
npm run setup --prefix bot
```
   Follow the prompts to supply credentials (press Enter to accept the suggested defaults). The scripts will scaffold `api/.env` and `bot/.env`; review the files afterwards for any remaining blanks. If you maintain additional workers, copy one of the templates and adjust as needed.

1. **Set up API Server**

   ```bash
   cd api
   npm install
   npm start
   ```
1. **Set up Bot**

   ```bash
   cd ../bot
   npm install
   npm start
   ```
1. **Start using the bot**
- Message your bot on Telegram
- Use `/start` to begin
- Use `/call` to make your first AI call

## 📱 Mini App (Telegram WebApp Admin Console)

The Mini App is the primary admin/authorized-user UI for VOICEDNUT. It provides
live inbound/outbound call status, answer/decline actions, call logs, and per-call
console timelines. It authenticates with Telegram Mini Apps initData and uses the
`/webapp/*` API namespace.

### Requirements
- Node.js 18+ (or 20+) and npm
- Telegram bot token configured in the API (`config.telegram.botToken` or `TELEGRAM_BOT_TOKEN`)
- Admin chat ID(s) set in the API (`TELEGRAM_ADMIN_CHAT_ID(S)`)
- HTTPS URL for Telegram clients

### Quick start
```bash
cd miniapp
npm install
npm run dev
```

Open the dev URL in Telegram (Mini App URL) or a browser:
- Dev URL: `http://localhost:5173/`

> Note: Telegram requires HTTPS. Use a tunnel (ngrok, Cloudflared, etc.) for device testing.

### Configure API base URL
Set the Mini App API base and socket base at build time:
```
VITE_API_BASE=https://api.example.com/template/api
VITE_SOCKET_URL=https://api.example.com
```

### API endpoints used
- `POST /webapp/auth` (initData auth, returns short-lived JWT)
- `GET /webapp/me`
- `GET /webapp/sse` (SSE live updates)
- `GET /webapp/calls?limit&cursor&status&q`
- `GET /webapp/calls/:callSid`
- `GET /webapp/calls/:callSid/events?after=`
- `GET /webapp/inbound/queue`
- `POST /webapp/inbound/:callSid/answer`
- `POST /webapp/inbound/:callSid/decline`
- `GET /webapp/scripts`
- `POST /webapp/scripts`
- `PUT /webapp/scripts/:id`
- `DELETE /webapp/scripts/:id`
- `GET /webapp/users` (admin allowlist)
- `POST /webapp/users`
- `POST /webapp/users/:id/promote`
- `DELETE /webapp/users/:id`
- `GET /webapp/settings`
- `POST /webapp/settings/provider`

### Auth + security notes
- initData is verified on auth using the bot token; expired initData is rejected (default 120s).
- Only configured admin/viewer users are authorized.
- JWTs are short-lived (default 10-15 minutes); no secrets in client.

### Build
```bash
npm run build
```
Output goes to `miniapp/dist/`. Host it anywhere that can reach your API and
serve over HTTPS.

### Environment checklist (prod)
API (`api/.env`):
```
TELEGRAM_BOT_TOKEN=...
TELEGRAM_ADMIN_CHAT_ID=... (or TELEGRAM_ADMIN_CHAT_IDS=...)
MINIAPP_PUBLIC_URL=https://<miniapp-domain>
MINIAPP_ALLOWED_ORIGINS=https://<miniapp-domain>
MINIAPP_JWT_SECRET=... (or reuse API_SECRET)
MINIAPP_INITDATA_MAX_AGE_S=120
MINIAPP_JWT_TTL_S=900
CORS_ORIGINS=https://<miniapp-domain>
```

Bot (`bot/.env`):
```
MINIAPP_URL=https://<miniapp-domain>
```

## 🔐 Access gating & verification

- Guests can open `/start`, `/menu`, `/help`, and `/guide` to see what the bot offers; actions such as `/call`, `/sms`, `/email`, and `/calllog` remain locked until an admin approves them.
- Request access by contacting the admin (the bot provides a "Request Access" button wherever commands are locked), then reopen `/start` to see the granted menus.
- Admins can monitor access-denied activity in `bot/logs/access-denied.log` and via `/status`, which reports the last few blocked attempts and rate-limited users.
- **Manual verification checklist:**
  1. Guest account: open `/start`, `/menu`, `/help`, and `/guide`; confirm only navigational UI renders and action buttons show lock/CTA.
  2. Authorized user: run `/call`, `/sms`, `/email`, and `/calllog` to confirm flows still operate.
  3. Admin: verify provider/users/bulk menus still appear and `/status` shows denial metrics after triggering a denied action.
  4. Tail the log with `tail -f bot/logs/access-denied.log` to watch automatic audit entries.

## ⚙️ Configuration

### API Server Configuration (`api/.env`)

```env
# Twilio Configuration
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=your_auth_token
FROM_NUMBER=+1234567890

# Server Configuration
PORT=3000
SERVER=your-domain.com

# OpenRouter AI Configuration
OPENROUTER_API_KEY=sk-or-v1-xxx
OPENROUTER_MODEL=meta-llama/llama-3.1-8b-instruct:free
YOUR_SITE_URL=http://localhost:3000
YOUR_SITE_NAME=Voice Call Bot

# Deepgram Configuration
DEEPGRAM_API_KEY=your_deepgram_key
VOICE_MODEL=aura-asteria-en

# Optional Features
RECORDING_ENABLED=false
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
```

### Bot Configuration (`bot/.env`)

```env
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
ADMIN_TELEGRAM_ID=123456789
ADMIN_TELEGRAM_USERNAME=your_username
API_URL=http://localhost:3000
MINIAPP_URL=https://voicednut-mini.vercel.app
WEB_APP_SECRET=super-secret-shared-key
WEB_APP_PORT=8080
```

## 📖 API Documentation

### Core Endpoints

#### Health Check

```http
GET /health
```

#### Make Outbound Call

```http
POST /outbound-call
Content-Type: application/json

{
  "number": "+1234567890",
  "prompt": "You are a friendly sales assistant...",
  "first_message": "Hello! How can I help you today?",
  "user_chat_id": "123456789"
}
```

#### Get Call Details

```http
GET /api/calls/{call_sid}
```

#### List Recent Calls

```http
GET /api/calls?limit=20
```

### Database Schema

<details>
<summary>Click to view database tables</summary>

#### Calls Table

```sql
CREATE TABLE calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_sid TEXT UNIQUE NOT NULL,
    phone_number TEXT NOT NULL,
    status TEXT DEFAULT 'initiated',
    duration INTEGER,
    prompt TEXT,
    first_message TEXT,
    call_summary TEXT,
    ai_analysis TEXT,
    user_chat_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME,
    ended_at DATETIME
);
```

#### Transcripts Table

```sql
CREATE TABLE transcripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_sid TEXT NOT NULL,
    speaker TEXT NOT NULL, -- 'user' or 'ai'
    message TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    interaction_count INTEGER,
    FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
);
```

</details>

## 🤖 Bot Commands

### Basic Commands

- `/start` - Start or restart the bot
- `/call` - Start a new voice call
- `/calllog` - Browse call history, search, and events
- `/sms` - Open SMS center
- `/email` - Open Email center
- `/health` - Check bot and API health
- `/guide` - Show detailed usage guide
- `/help` - Show available commands
- `/menu` - Show quick action buttons

### Admin Commands

- `/adduser` - Add new authorized user
- `/promote` - Promote user to admin
- `/removeuser` - Remove user access
- `/users` - List all authorized users
- `/status` - Full system status check
- `/smssender` - Bulk SMS center
- `/mailer` - Bulk email center
- `/scripts` - Script designer (call/SMS/email)
- `/persona` - Manage personas
- `/provider` - Voice provider controls

## 📁 Project Structure

```
voice-call-bot/
├── api/                    # API Server (Express.js)
│   ├── app.js             # Main API application
│   ├── routes/            # API route handlers
│   │   ├── gpt.js         # AI integration
│   │   ├── stream.js      # Audio streaming
│   │   ├── transcription.js # Speech-to-text
│   │   ├── tts.js         # Text-to-speech
│   │   └── status.js      # Webhook service
│   ├── db/                # Database management
│   └── functions/         # AI function calling
│
├── bot/                   # Telegram Bot (Grammy.js)
│   ├── bot.js            # Main bot application
│   ├── commands/         # Bot command handlers
│   │   ├── call.js       # Voice call functionality
│   │   ├── transcript.js # Transcript management
│   │   ├── api.js        # API testing
│   │   └── ...
│   ├── db/               # Bot database
│   └── utils/            # Utility functions
│
├── miniapp/              # Telegram Mini App (Vite + React)
│   ├── src/              # Mini App UI
│   └── dist/             # Build output
│
└── README.md             # This documentation
```

## 🔧 Development

### Local Development

```bash
# Terminal 1 - API Server
cd api && npm run dev

# Terminal 2 - Bot
cd bot && npm run dev
```

### Production Deployment

#### Using PM2

```bash
npm install -g pm2

# Start services
cd api && pm2 start npm --name "voice-api" -- start
cd bot && pm2 start npm --name "voice-bot" -- start

# Save configuration
pm2 save && pm2 startup
```

#### Using Docker

```dockerfile
FROM node:16-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
```

## 🎯 Usage Examples

### Making Your First Call

1. **Start the bot**: Send `/start` to your bot
1. **Initiate a call**: Use `/call` command
1. **Enter details**:
- Phone number: `+1234567890`
- AI prompt: `"You are a friendly customer service agent"`
- First message: `"Hello! How can I help you today?"`
1. **Monitor progress**: Receive real-time notifications
1. **Get transcript**: Use `/transcript <call_sid>` after the call

### AI Model Configuration

Available free models through OpenRouter:

- `meta-llama/llama-3.1-8b-instruct:free` (default)
- `microsoft/wizardlm-2-8x22b`
- `google/gemma-2-9b-it:free`
- `qwen/qwen-2-7b-instruct:free`

### Voice Models (Deepgram)

Available TTS voices:

- `aura-asteria-en` - Friendly female (default)
- `aura-orion-en` - Friendly male
- `aura-luna-en` - Warm female
- `aura-arcas-en` - Professional male
- And many more…

## 🛠️ Troubleshooting

<details>
<summary>Common Issues & Solutions</summary>

### Bot not responding

- ✅ Check bot token in `.env`
- ✅ Verify bot is running with `npm start`
- ✅ Ensure user is authorized

### API connection failed

- ✅ Check API server is running
- ✅ Verify `API_BASE` URL in bot `.env`
- ✅ Test with `/test_api` command

### Calls not working

- ✅ Verify Twilio credentials
- ✅ Check phone number has voice capabilities
- ✅ Ensure webhook URL is accessible
- ✅ Use ngrok for local testing: `ngrok http 3000`

### AI responses not working

- ✅ Check OpenRouter API key
- ✅ Verify model name and availability
- ✅ Ensure sufficient credits

</details>

## 🔒 Security

- User authorization required for all operations
- Admin-only commands protected
- Input validation for all user inputs
- Secure phone number format validation
- Protected sensitive information in logs
- Webhook signature verification

## 📊 Monitoring

### Health Checks

- API: `GET /health`
- Bot: `/health` command
- Admin: `/status` command
- Admin: `/provider <name>|status` to inspect or switch the active call backbone

### Logging

- Structured logging with timestamps
- Error tracking and debugging
- Call analytics and metrics
- Performance monitoring

## 🤝 Contributing

1. Fork the repository
1. Create a feature branch (`git checkout -b feature/amazing-feature`)
1. Commit your changes (`git commit -m 'Add amazing feature'`)
1. Push to the branch (`git push origin feature/amazing-feature`)
1. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the <LICENSE> file for details.

## 🙋‍♂️ Support

- 📖 **Documentation**: Check this README and inline code comments
- 🐛 **Issues**: [GitHub Issues](https://github.com/yourusername/voice-call-bot/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/yourusername/voice-call-bot/discussions)
- 📧 **Contact**: Your contact information

## 🎉 Acknowledgments

- [Twilio](https://www.twilio.com/) for voice communication infrastructure
- [OpenRouter](https://openrouter.ai/) for AI model access
- [Deepgram](https://deepgram.com/) for speech services
- [Grammy](https://grammy.dev/) for Telegram bot framework
- All contributors who helped build this project

## 📮 Voicemail QA

See `docs/voicemail-qa.md` for a quick checklist and sample webhook payloads to validate voicemail detection behavior.

## 📊 Project Status

- ✅ **Stable**: Core functionality working
- 🔄 **Active Development**: Regular updates and improvements
- 🐛 **Bug Reports**: Welcome and addressed promptly
- 🚀 **Feature Requests**: Open to new ideas and suggestions

-----

<div align="center">

**⭐ If you find this project helpful, please consider giving it a star! ⭐**

[Report Bug](https://github.com/yourusername/voice-call-bot/issues) · [Request Feature](https://github.com/yourusername/voice-call-bot/issues) · [Contribute](CONTRIBUTING.md)

</div>
