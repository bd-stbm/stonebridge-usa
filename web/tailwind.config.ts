import type { Config } from "tailwindcss";

// Brand colours sampled from /public/stonebridge-logo.png — vivid violet
// for primary surfaces (active controls, portfolio line, etc.); darker
// variant for hover; lighter for tint backgrounds.
const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          DEFAULT: "#8b5cf6",
          dark: "#7c3aed",
          light: "#a78bfa",
          tint: "#ede9fe",
          navy: "#1e1b4b",
        },
      },
    },
  },
  plugins: [],
};
export default config;
