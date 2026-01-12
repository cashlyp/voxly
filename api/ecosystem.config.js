module.exports = {
  apps: [
    {
      name: 'API',
      script: './app.js',
      cwd: '/home/ubuntu/voicednut/api', // Update with your actual path
      instances: 1, // You can scale this if needed
      exec_mode: 'fork', // Use 'cluster' for load balancing if needed
      
      // Environment variables
      env: {
        NODE_ENV: 'production',
        PORT: 1337,
      },
      
      // Restart policy
      restart_delay: 2000,
      max_restarts: 5,
      min_uptime: '10s',
      
      // Logging
      log_file: '/home/ubuntu/voicednut/logs/api/combined.log',
      out_file: '/home/ubuntu/voicednut/logs/api/out.log',
      error_file: '/home/ubuntu/voicednut/logs/api/error.log',
      log_type: 'json',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      
      // Advanced options
      watch: false, // Set to true in development
      ignore_watch: [
        'node_modules',
        'logs',
        'db/*.db',
        '.git'
      ],
      
      // Memory and CPU limits
      max_memory_restart: '1G',
      
      // Process management
      kill_timeout: 5000,
      wait_ready: true,
      listen_timeout: 3000,
      
      // Health monitoring
      health_check_grace_period: 3000,
      
      // Auto restart conditions
      node_args: '--max-old-space-size=1024',
      
      // Merge logs
      merge_logs: true,
      
      // Time zone
      time: true,
      
      // Auto restart on file changes (development only)
      autorestart: true,
      
      // Environment-specific settings
      env_production: {
        NODE_ENV: 'production',
        PORT: 1337,
        // Add your production environment variables here
        // SERVER: process.env.SERVER,
        // TWILIO_ACCOUNT_SID: process.env.TWILIO_ACCOUNT_SID,
        // TWILIO_AUTH_TOKEN: process.env.TWILIO_AUTH_TOKEN,
        // FROM_NUMBER: process.env.FROM_NUMBER,
        // OPENAI_API_KEY: process.env.OPENAI_API_KEY
      },
      
      env_development: {
        NODE_ENV: 'development',
        PORT: 1337,
        watch: true
      },
      
      // Error handling
      crash_restart_delay: 1000,
      
      // Custom startup script (if needed)
      // pre_restart_script: './scripts/pre-restart.sh',
      // post_restart_script: './scripts/post-restart.sh'
    }
  ],
  
  deploy: {
    production: {
      user: 'ubuntu',
      host: 'ec2-18-118-121-26.us-east-2.compute.amazonaws.com', // Update with your EC2 public IP or domain
      ref: 'origin/main',
      repo: 'git@github.com:ednutlabs/voicednut.git', // Update with your repo
      path: '/home/ubuntu/voicednut',
      'pre-setup': 'apt-get install git -y',
      'post-setup': 'ls -la',
      'pre-deploy': 'pm2 startOrRestart ecosystem.config.js --env production',
      'post-deploy': 'npm install && pm2 reload ecosystem.config.js --env production && pm2 save'
    }
  }
};
