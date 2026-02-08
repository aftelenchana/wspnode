module.exports = {
  apps: [{
    name: 'wsp',
    script: './server.js',
    // Activa watch, pero ignora carpetas que cambian constantemente
    watch: true,
    watch_delay: 1500,
    ignore_watch: [
      'sessions',
      'qrcodes',
      'files',
      'campanas',
      'node_modules',
      '.git'
    ],
    env: { NODE_ENV: 'production' }
  }]
}
