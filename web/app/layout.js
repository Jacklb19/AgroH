import "./globals.css";
import Nav from "./components/Nav";
import Footer from "./components/Footer";

export const metadata = {
  title: {
    default: "AgroIA Colombia — Predicción agroclimática con datos abiertos",
    template: "%s · AgroIA Colombia",
  },
  description:
    "Anticipa el rendimiento de un cultivo y su riesgo climático en cualquier municipio de Colombia, con datos abiertos del DANE, IDEAM, UPRA y NOAA.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="es">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@500;600;700&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600&display=swap"
          rel="stylesheet"
        />
        <link rel="stylesheet" href="/styles.css" />
      </head>
      <body>
        <a href="#main" className="skip-link">Saltar al contenido principal</a>
        <div className="app">
          <Nav />
          <main id="main" className="main" tabIndex={-1}>{children}</main>
          <Footer />
        </div>
      </body>
    </html>
  );
}
