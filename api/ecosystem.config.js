const path = require('path');

const rootDir = path.resolve(__dirname, '..');
const logDir = path.join(rootDir, 'logs', 'api');

module.exports = {
  apps: [
    {
      name: 'API',
      script: 'app.js',
      cwd: '/home/ubuntu/voxly/api',
      instances: 1,
      exec_mode: 'fork',

      env: {
        NODE_ENV: 'production',
        PORT: 1337,
        FORCE_COLOR: '1',
      },

      autorestart: true,
      restart_delay: 3000,
      exp_backoff_restart_delay: 200,
      max_restarts: 15,
      min_uptime: '15s',
      kill_timeout: 10000,
      max_memory_restart: '1G',
      node_args: '--max-old-space-size=1024',

      watch: false,
      ignore_watch: ['node_modules', 'logs', 'db/*.db', '.git'],

      log_file: path.join(logDir, 'combined.log'),
      out_file: path.join(logDir, 'out.log'),
      error_file: path.join(logDir, 'error.log'),
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      time: true,
    },
  ],

  deploy: {
    production: {
      user: 'ubuntu',
      host: '3.135.63.195',
      ref: 'origin/main',
      repo: 'git@github.com:cashlyp/voxly.git',
      path: '/home/ubuntu/voxly',
      'pre-setup': 'apt-get install git -y',
      'post-setup': 'ls -la',
      'pre-deploy': 'pm2 startOrRestart ecosystem.config.js --env production',
      'post-deploy':
        'npm install && pm2 reload ecosystem.config.js --env production && pm2 save',
    },
  },
};
