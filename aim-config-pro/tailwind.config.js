/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
        "./components/**/*.{js,ts,jsx,tsx}" // Just in case, though components are usually in src
    ],
    darkMode: "class",
    theme: {
        extend: {
            colors: {
                primary: "#00AB04", // BC Green
                "bc-black": "#1A171B", // BC Black
                "bc-light-grey": "#F4F4F4", // Light Grey
                "bc-text-grey": "#767676",
                "background-light": "#FFFFFF",
                "background-dark": "#1A171B", // BC Black for dark mode background
            },
            fontFamily: {
                sans: ["Open Sans", "sans-serif"],
                mono: ["Fira Code", "monospace"],
            },
            borderRadius: {
                DEFAULT: "0.5rem",
            },
            boxShadow: {
                'ramp': '0 4px 14px 0 rgba(0, 171, 4, 0.39)',
                'soft': '0 2px 8px rgba(0,0,0,0.06)',
            }
        },
    },
    plugins: [],
}
