const path = require('path');

const rootDir = path.resolve(__dirname, '..');
const logDir = path.join(rootDir, 'logs', 'bot');

module.exports = {
  apps: [
    {
      name: 'BOT',
      script: 'bot.js',
      cwd: '/home/ubuntu/voxly/bot',
      instances: 1,
      exec_mode: 'fork',

      env: {
        NODE_ENV: 'production',
      },

      autorestart: true,
      restart_delay: 3000,
      exp_backoff_restart_delay: 200,
      max_restarts: 15,
      min_uptime: '15s',
      kill_timeout: 10000,
      max_memory_restart: '1G',

      watch: false,
      ignore_watch: ['node_modules', 'logs', 'db/*.db', '.git'],

      log_file: path.join(logDir, 'combined.log'),
      out_file: path.join(logDir, 'out.log'),
      error_file: path.join(logDir, 'error.log'),
      log_type: 'json',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      time: true,
    },
  ],
};
