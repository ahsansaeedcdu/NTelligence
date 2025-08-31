import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(),tailwindcss(),],
  server: {
    host: true, // listen on 0.0.0.0 so ngrok can reach it
    // allow your tunnel host (the leading dot allows any subdomain)
    allowedHosts: ['.ngrok-free.app', 'a1d81dbf4840.ngrok-free.app'],
    // make HMR use the tunnel over secure websockets
    hmr: {
      host: 'a1d81dbf4840.ngrok-free.app',
      protocol: 'wss',
      clientPort: 443
    }
  },
  // if you ever use `vite preview`, keep it allowed there too:
  preview: {
    allowedHosts: ['.ngrok-free.app']
  }
})
