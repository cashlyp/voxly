module.exports = {
  apps: [
    {
      name: 'BOT',
      script: './bot.js',
      cwd: '/home/ubuntu/voicednut/bot',
      instances: 1, // ✅ Single instance
      exec_mode: 'fork',

      env: {
        NODE_ENV: 'production',
      },

      restart_delay: 2000,
      max_restarts: 5,
      min_uptime: '10s',

      log_file: '/home/ubuntu/voicednut/logs/bot/combined.log',
      out_file: '/home/ubuntu/voicednut/logs/bot/out.log',
      error_file: '/home/ubuntu/voicednut/logs/bot/error.log',
      log_type: 'json',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',

      watch: false,
      ignore_watch: ['node_modules', 'logs', 'db/*.db', '.git'],

      max_memory_restart: '1G',
      kill_timeout: 5000,

      // 🔥 Removed wait_ready and listen_timeout
      // wait_ready: true,
      // listen_timeout: 3000,

      health_check_grace_period: 3000,
      merge_logs: true,
      time: true,
      autorestart: true,

      env_production: {
        NODE_ENV: 'production',
        PORT: 1337,
      },

      env_development: {
        NODE_ENV: 'development',
        watch: true
      },

      crash_restart_delay: 1000
    }
  ]
};
