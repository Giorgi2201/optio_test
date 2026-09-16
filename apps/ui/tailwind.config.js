/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      // Utilitarian infrastructure console palette: deep zinc surfaces, functional accents only.
      colors: {
        surface: {
          base: '#09090b', // zinc-950
          raised: '#18181b', // zinc-900
          border: '#27272a' // zinc-800
        }
      },
      fontFamily: {
        sans: [
          'Inter',
          'ui-sans-serif',
          'system-ui',
          '-apple-system',
          'Segoe UI',
          'Roboto',
          'Helvetica Neue',
          'Arial',
          'sans-serif'
        ],
        mono: [
          'JetBrains Mono',
          'ui-monospace',
          'SFMono-Regular',
          'Menlo',
          'Consolas',
          'Liberation Mono',
          'monospace'
        ]
      },
      fontSize: {
        '2xs': ['0.6875rem', { lineHeight: '1rem' }]
      }
    }
  },
  plugins: []
};
