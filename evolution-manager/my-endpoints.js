// "My Endpoints" add-on for the Evolution Manager (served at /assets/my-endpoints.js).
// Adds a sidebar item on instance pages that opens a panel listing the notifier API's endpoints,
// read live from its /openapi.json, plus ready-made message URLs for every WhatsApp group.
(() => {
  const STORE_KEY = "myEndpoints.apiBase";
  const DEFAULT_BASE = `${location.protocol}//${location.hostname}:6025`;
  const NAV_ID = "my-endpoints-nav";
  const PANEL_ID = "my-endpoints-panel";

  const apiBase = () => {
    try { return localStorage.getItem(STORE_KEY) || DEFAULT_BASE; } catch { return DEFAULT_BASE; }
  };
  const setApiBase = (v) => { try { localStorage.setItem(STORE_KEY, v); } catch { /* private mode */ } };

  const el = (tag, style = {}, text) => {
    const n = document.createElement(tag);
    Object.assign(n.style, style);
    if (text !== undefined) n.textContent = text;
    return n;
  };

  // Colours come from the manager's own theme variables, so light and dark mode both work.
  const C = {
    bg: "hsl(var(--background))", fg: "hsl(var(--foreground))", muted: "hsl(var(--muted-foreground))",
    border: "hsl(var(--border))", card: "hsl(var(--card, var(--background)))", accent: "hsl(var(--accent))",
    primary: "hsl(var(--primary))",
  };
  const METHOD_COLORS = { GET: "#2563eb", POST: "#16a34a", PUT: "#d97706", PATCH: "#d97706", DELETE: "#dc2626" };

  function button(label, onClick) {
    const b = el("button", {
      border: `1px solid ${C.border}`, background: "transparent", color: C.fg, borderRadius: "6px",
      padding: "4px 10px", fontSize: "12px", cursor: "pointer", whiteSpace: "nowrap",
    }, label);
    b.addEventListener("click", onClick);
    return b;
  }

  function urlRow(url, { openable = true } = {}) {
    const row = el("div", { display: "flex", gap: "6px", alignItems: "center", marginTop: "6px", flexWrap: "wrap" });
    const code = el("code", {
      flex: "1 1 260px", minWidth: "0", overflowWrap: "anywhere", fontSize: "12px", padding: "6px 8px",
      background: C.accent, borderRadius: "6px", color: C.fg,
    }, url);
    row.append(code);
    const copy = button("Copy", async () => {
      try { await navigator.clipboard.writeText(url); copy.textContent = "Copied"; }
      catch { copy.textContent = "Copy failed"; }
      setTimeout(() => (copy.textContent = "Copy"), 1500);
    });
    row.append(copy);
    if (openable) row.append(button("Open", () => window.open(url, "_blank", "noopener")));
    return row;
  }

  function endpointCard(method, path, op, base) {
    const card = el("div", { border: `1px solid ${C.border}`, borderRadius: "8px", padding: "12px", marginBottom: "10px" });
    const head = el("div", { display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" });
    head.append(
      el("span", {
        background: METHOD_COLORS[method] || "#6b7280", color: "#fff", borderRadius: "4px",
        padding: "2px 6px", fontSize: "11px", fontWeight: "700",
      }, method),
      el("span", { fontFamily: "monospace", fontSize: "13px", fontWeight: "600", color: C.fg }, path),
    );
    card.append(head);
    const summary = op.summary ? op.summary : "";
    const desc = (op.description || "").trim();
    if (summary) card.append(el("div", { fontSize: "13px", fontWeight: "600", marginTop: "6px", color: C.fg }, summary));
    if (desc) card.append(el("div", { fontSize: "13px", marginTop: "4px", color: C.muted, whiteSpace: "pre-line" }, desc));
    const params = (op.parameters || []).filter((p) => p.in === "query");
    if (params.length) {
      const list = el("div", { fontSize: "12px", marginTop: "6px", color: C.muted });
      list.append(el("div", { fontWeight: "600" }, "Query parameters:"));
      for (const p of params) {
        list.append(el("div", {}, `• ${p.name}${p.required ? " (required)" : " (optional)"}${p.description ? " — " + p.description : ""}`));
      }
      card.append(list);
    }
    if (op.requestBody) {
      card.append(el("div", { fontSize: "12px", marginTop: "6px", color: C.muted },
        "Takes a JSON body; try it from the API docs page."));
    }
    // GET routes without path parameters can be opened directly.
    card.append(urlRow(base + path, { openable: method === "GET" && !path.includes("{") }));
    return card;
  }

  async function fetchJson(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  }

  async function render(body) {
    body.replaceChildren(el("div", { color: C.muted, fontSize: "13px" }, "Loading…"));
    const base = apiBase().replace(/\/+$/, "");
    let spec;
    try {
      spec = await fetchJson(`${base}/openapi.json`);
    } catch (e) {
      body.replaceChildren(el("div", { color: "#dc2626", fontSize: "13px" },
        `Could not reach the notifier API at ${base} (${e.message}). Is it running, and is this address right?`));
      return;
    }
    body.replaceChildren();

    body.append(el("div", { fontSize: "13px", color: C.muted, marginBottom: "10px" },
      `${spec.info?.title || "API"} — ${base}`));
    body.append(urlRow(`${base}/docs`));
    body.append(el("div", { fontSize: "12px", color: C.muted, margin: "4px 0 14px" },
      "Interactive docs: try every endpoint from the browser."));

    // Group message URLs first: the thing people come here for.
    const groupsSection = el("div", { marginBottom: "16px" });
    groupsSection.append(el("h3", { fontSize: "14px", fontWeight: "700", margin: "0 0 8px", color: C.fg }, "Group messages"));
    groupsSection.append(el("div", { fontSize: "12px", color: C.muted, marginBottom: "6px" },
      "Every stored message per group. Add ?start=YYYY-MM-DD and/or &end=YYYY-MM-DD to filter by date."));
    body.append(groupsSection);
    try {
      const groups = await fetchJson(`${base}/whatsapp/groups`);
      if (!groups.length) groupsSection.append(el("div", { fontSize: "13px", color: C.muted }, "No groups found."));
      for (const g of groups) {
        const box = el("div", { border: `1px solid ${C.border}`, borderRadius: "8px", padding: "10px", marginBottom: "8px" });
        box.append(el("div", { fontSize: "13px", fontWeight: "600", color: C.fg }, g.name || g.group_id));
        box.append(el("div", { fontSize: "11px", color: C.muted }, g.group_id));
        box.append(urlRow(`${base}/whatsapp/groups/${encodeURIComponent(g.group_id)}/messages`));
        groupsSection.append(box);
      }
    } catch (e) {
      groupsSection.append(el("div", { fontSize: "13px", color: C.muted }, `Group list unavailable (${e.message}).`));
    }

    body.append(el("h3", { fontSize: "14px", fontWeight: "700", margin: "0 0 8px", color: C.fg }, "All endpoints"));
    for (const [path, ops] of Object.entries(spec.paths || {})) {
      for (const [method, op] of Object.entries(ops)) {
        body.append(endpointCard(method.toUpperCase(), path, op, base));
      }
    }
  }

  function openPanel() {
    document.getElementById(PANEL_ID)?.remove();
    const overlay = el("div", {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.45)", zIndex: "1000",
      display: "flex", justifyContent: "flex-end",
    });
    overlay.id = PANEL_ID;
    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });

    const panel = el("div", {
      width: "min(640px, 100vw)", height: "100%", background: C.bg, color: C.fg, borderLeft: `1px solid ${C.border}`,
      display: "flex", flexDirection: "column", boxShadow: "-8px 0 24px rgba(0,0,0,0.2)",
    });
    const header = el("div", {
      display: "flex", alignItems: "center", gap: "8px", padding: "14px 16px", borderBottom: `1px solid ${C.border}`,
    });
    header.append(el("div", { fontSize: "16px", fontWeight: "700", flex: "1" }, "My Endpoints"));
    header.append(button("Refresh", () => render(body)));
    header.append(button("Close", () => overlay.remove()));

    const settings = el("div", { display: "flex", gap: "6px", alignItems: "center", padding: "10px 16px",
      borderBottom: `1px solid ${C.border}`, flexWrap: "wrap" });
    settings.append(el("label", { fontSize: "12px", color: C.muted }, "API address"));
    const input = el("input", {
      flex: "1 1 200px", fontSize: "12px", padding: "5px 8px", borderRadius: "6px",
      border: `1px solid ${C.border}`, background: C.bg, color: C.fg,
    });
    input.value = apiBase();
    settings.append(input, button("Save", () => { setApiBase(input.value.trim() || DEFAULT_BASE); render(body); }));

    const body = el("div", { flex: "1", overflowY: "auto", padding: "16px" });
    panel.append(header, settings, body);
    overlay.append(panel);
    document.body.append(overlay);
    render(body);
  }

  function addNavItem() {
    if (document.getElementById(NAV_ID)) return;
    // The instance sidebar is the <ul> with a right border; its "Documentation" item sits at the bottom (mt-auto).
    const list = [...document.querySelectorAll("ul.border-r")].find((u) => u.querySelector("li"));
    if (!list) return;
    const li = el("li");
    li.id = NAV_ID;
    const b = el("button", {
      display: "flex", width: "100%", alignItems: "center", gap: "8px", padding: "8px 16px", fontSize: "14px",
      fontWeight: "500", background: "transparent", border: "none", color: C.primary, cursor: "pointer",
      borderRadius: "6px", textAlign: "left",
    });
    b.innerHTML = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
      'stroke-linecap="round" stroke-linejoin="round"><path d="m18 16 4-4-4-4"/><path d="m6 8-4 4 4 4"/>' +
      '<path d="m14.5 4-5 16"/></svg><span>My Endpoints</span>';
    b.addEventListener("mouseenter", () => (b.style.background = C.accent));
    b.addEventListener("mouseleave", () => (b.style.background = "transparent"));
    b.addEventListener("click", openPanel);
    li.append(b);
    const bottom = list.querySelector(":scope > li.mt-auto");
    list.insertBefore(li, bottom || null);
  }

  // The manager is a single-page app that re-renders the sidebar on navigation: keep the item present.
  new MutationObserver(addNavItem).observe(document.documentElement, { childList: true, subtree: true });
  addNavItem();
})();
