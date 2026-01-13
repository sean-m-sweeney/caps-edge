/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./frontend/**/*.{html,js}",
  ],
  theme: {
    extend: {
      fontFamily: {
        'arcade': ['"Press Start 2P"', 'cursive'],
        'mono': ['"JetBrains Mono"', 'monospace'],
      },
      colors: {
        'void': '#09090b',
        'void-light': '#18181b',
        'neon-cyan': '#00f3ff',
        'neon-pink': '#ff00ff',
        'neon-green': '#39ff14',
        'grid-line': '#27272a',
      }
    }
  },
  plugins: [],
}
