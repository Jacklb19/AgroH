"use client";
import { useState } from "react";
import Nav from "./components/Nav";
import Footer from "./components/Footer";
import PageInicio from "./components/PageInicio";
import PageDashboards from "./components/PageDashboards";
import PagePrediccion from "./components/PagePrediccion";
import PageMetodologia from "./components/PageMetodologia";
import PageImpacto from "./components/PageImpacto";
import PageAsistente from "./components/PageAsistente";

export default function App() {
  const [active, setActive] = useState("inicio");

  const onNav = (page) => {
    setActive(page);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  return (
    <>
      <Nav active={active} onNav={onNav} />
      <main className="main">
        {active === "inicio"      && <PageInicio      onNav={onNav} />}
        {active === "dashboards"  && <PageDashboards  />}
        {active === "prediccion"  && <PagePrediccion  />}
        {active === "asistente"   && <PageAsistente   />}
        {active === "metodologia" && <PageMetodologia />}
        {active === "impacto"     && <PageImpacto     onNav={onNav} />}
      </main>
      <Footer onNav={onNav} />
    </>
  );
}
