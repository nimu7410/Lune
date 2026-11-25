/* Lune · console — vanilla, no dependencies. One page for both books: v3's
   night trade and the HH/LL desk's day trades, each tagged with its book.
   The canvas palette is read out of Lune-v5.css, so the chart cannot drift a
   theme behind the page around it. */
(() => {
  "use strict";

  const body = document.body;
  const timeframe = body.dataset.timeframe || "5m";
  const snapshotPollMs = Math.max(750, Number(body.dataset.pollMs) || 1000);
  const $ = (id) => document.getElementById(id);

  // Which of v3's indices this console is pointed at. The server names the
  // ones whose feed actually resolved; a remembered choice is honoured only if
  // it is still one of them, so a stored name cannot outlive its feed.
  const INDICES = (body.dataset.indices || "").split(",").map((s) => s.trim()).filter(Boolean);
  const DEFAULT_INDEX = body.dataset.index || INDICES[0] || "";
  const remembered = (() => {
    try { return window.localStorage.getItem("lune.index"); } catch (_e) { return null; }
  })();
  let index = INDICES.includes(remembered) ? remembered : DEFAULT_INDEX;
  const forIndex = (path) => (index ? `${path}?index=${encodeURIComponent(index)}` : path);

  function announceIndex() {
    body.dataset.index = index;
    document.dispatchEvent(new CustomEvent("lune:index", { detail: index }));
  }

  // The engine bay is switched on its own. v3 gives its single slot to the
  // first qualifying index in its waterfall, so "is SENSEX clear tonight" is
  // worth asking without giving up the candles already on screen. It follows
  // the chart when the chart moves and stays where it is put otherwise.
  let engineIndex = index;
  let engines = {};

  // The 14:50 bar, minutes since midnight: the last bar that can take the
  // overnight carry (v3 also takes it on 14:45). Mirrors CARRY_TOD in
  // strategy_logic.py.
  const CARRY_TOD = 890;

  const root = getComputedStyle(document.documentElement);
  const v = (n, f) => (root.getPropertyValue(n) || f).trim();

  const P = {
    up: v("--up", "#35e6a4"),
    upFill: v("--up-fill", "rgba(53,230,164,.9)"),
    upLine: v("--up-line", "rgba(53,230,164,.4)"),
    upSoft: v("--up-soft", "rgba(53,230,164,.14)"),
    down: v("--down", "#ff6b8b"),
    downFill: v("--down-fill", "rgba(255,107,139,.9)"),
    downLine: v("--down-line", "rgba(255,107,139,.4)"),
    downSoft: v("--down-soft", "rgba(255,107,139,.14)"),
    axis: v("--axis", "#7181a3"),
    grid: v("--grid", "rgba(255,255,255,.055)"),
    gridSoft: v("--grid-strong", "rgba(255,255,255,.03)"),
    cross: v("--cross", "rgba(234,240,255,.34)"),
    crossDot: v("--cross-dot", "#eaf0ff"),
    badgeInk: v("--ink-badge", "#05070f"),
    mono: v("--mono", "ui-monospace, monospace"),
  };

  const el = {
    feedPod: $("feedPod"), feedLabel: $("feedLabel"),
    pingPod: $("pingPod"), pingLadder: $("pingLadder"), pingValue: $("pingValue"),
    modePod: $("modePod"), modeValue: $("modeValue"), clock: $("clock"),
    quote: $("quote"), quoteIndex: $("quoteIndex"), priceMain: $("priceMain"), pricePaise: $("pricePaise"),
    delta: $("delta"), asOf: $("asOf"),
    o: $("o"), h: $("h"), l: $("l"), c: $("c"), candleAt: $("candleAt"), barCount: $("barCount"),
    veil: $("veil"), tip: $("tip"),
    engineBay: $("engineBay"), bias: $("bias"), biasMeta: $("biasMeta"),
    verdict: $("verdict"), gates: $("gates"),
    carry: $("carry"), carryNote: $("carryNote"), carryScore: $("carryScore"),
    enginePicker: $("enginePicker"),
    pnl: $("pnl"), pnlMeta: $("pnlMeta"), closed: $("closed"),
    legs: $("legs"), legsTag: $("legsTag"), legsNote: $("legsNote"),
    healthTag: $("healthTag"),
    brokerLed: $("brokerLed"), brokerPing: $("brokerPing"), brokerMeta: $("brokerMeta"), spark: $("spark"),
    wsLed: $("wsLed"), wsRate: $("wsRate"), wsMeta: $("wsMeta"), feeds: $("feeds"),
    botLed: $("botLed"), botCpu: $("botCpu"), dbLed: $("dbLed"), dbCpu: $("dbCpu"), hostMeta: $("hostMeta"),
    toast: $("toast"),
    picker: document.querySelector(".head .picker"),
  };

  el.quoteIndex.textContent = index;

  // The name is set in the display size the column was drawn for, which is
  // NIFTY's; MIDCPNIFTY or NIFTYNXT50 at that size runs into the price. It is
  // scaled down to its column instead, never up past the stylesheet's size.
  function fitIndexName() {
    const h = el.quoteIndex;
    h.style.fontSize = "";
    // The text's own box, not the element's: with the negative tracking the
    // last glyph inks a few pixels past the element's scroll width.
    const text = document.createRange();
    text.selectNodeContents(h);
    const room = h.clientWidth, need = text.getBoundingClientRect().width;
    if (room > 0 && need > room)
      h.style.fontSize = `${Math.floor(parseFloat(getComputedStyle(h).fontSize) * (room / need) * 0.98 * 10) / 10}px`;
  }

  // ── Formatting ───────────────────────────────────────────────────────────
  const nf = new Intl.NumberFormat("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const IST = { timeZone: "Asia/Kolkata", hourCycle: "h23" };
  const fClock = new Intl.DateTimeFormat("en-GB", { ...IST, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const fHm = new Intl.DateTimeFormat("en-GB", { ...IST, hour: "2-digit", minute: "2-digit" });
  const fDayHm = new Intl.DateTimeFormat("en-GB", { ...IST, day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" });

  const num = (x) => { const n = Number(x); return Number.isFinite(n) ? n : null; };
  const fmt = (x) => (Number.isFinite(x) ? nf.format(x) : "—");
  const signed = (x, s = "") => (Number.isFinite(x) ? `${x > 0 ? "+" : ""}${nf.format(x)}${s}` : "—");

  function tone(node, value) {
    node.classList.remove("up", "down", "flat");
    node.classList.add(value > 0 ? "up" : value < 0 ? "down" : "flat");
  }

  // Candle and pivot times arrive as IST wall clock. Current builds label them
  // "+05:30"; older ones published the same digits with a "Z" that also meant
  // IST. Either way the digits are the truth and the suffix is ignored, so a
  // page held open across a deploy cannot plot two sets of bars five and a half
  // hours apart. Shifting out of IST makes the result a real instant, which is
  // what lets the formatters render it honestly.
  const IST_MS = 5.5 * 3600 * 1000;

  function parseFeedTime(x) {
    const m = String(x ?? "").match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?/);
    return m ? Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +(m[6] || 0)) - IST_MS : Number.NaN;
  }

  const fTime = (ts, withDate = false) =>
    Number.isFinite(ts) ? (withDate ? fDayHm : fHm).format(new Date(ts)) : "—";

  function normalizeCandles(rows) {
    if (!Array.isArray(rows)) return [];
    return rows.map((r) => {
      const open = num(r?.open), high = num(r?.high), low = num(r?.low), close = num(r?.close);
      const timestamp = parseFeedTime(r?.time);
      if ([open, high, low, close].some((x) => x === null) || !Number.isFinite(timestamp)) return null;
      return { timestamp, open, high, low, close };
    }).filter(Boolean).sort((a, b) => a.timestamp - b.timestamp);
  }

  function normalizePivots(points, type) {
    if (!Array.isArray(points)) return [];
    return points.map((p) => {
      const price = num(p?.price ?? p?.[type === "HH" ? "high" : "low"]);
      const timestamp = parseFeedTime(p?.time);
      return price === null || !Number.isFinite(timestamp) ? null : { type, price, timestamp };
    }).filter(Boolean).sort((a, b) => a.timestamp - b.timestamp);
  }

  async function fetchJson(path, timeoutMs = 4500) {
    const ac = new AbortController();
    const timer = window.setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(path, { cache: "no-store", headers: { Accept: "application/json" }, signal: ac.signal });
      if (!res.ok) throw new Error(`${path} ${res.status}`);
      return await res.json();
    } finally { window.clearTimeout(timer); }
  }

  let toastTimer = 0;
  function toast(message) {
    el.toast.textContent = message;
    el.toast.classList.add("is-visible");
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => el.toast.classList.remove("is-visible"), 4200);
  }

  // ── Chart ────────────────────────────────────────────────────────────────
  // The strip below the plot that the time axis is drawn into.
  const AXIS_ROW = 18;
  const MIN_BARS = 10;          // zoomed all the way in
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

  // The view, panned and zoomed by drag, wheel, pinch and keys: `span` bars
  // wide (null: every bar) ending `offset` bars before the newest, the price
  // range scaled by `yZoom` (dragging the price scale). An offset of 0 follows
  // the market; double-click or End returns there.
  class Chart {
    constructor(canvas, tip) {
      this.canvas = canvas;
      this.tip = tip;
      this.ctx = canvas.getContext("2d", { alpha: true });
      this.candles = []; this.pivots = [];
      this.hover = null; this.hoverY = null;
      this.geom = null; this.frame = null; this.onSelect = null;
      this.inset = { top: 34, right: 74, bottom: 26, left: 14 };
      this.axisGuard = 14;
      this.pointers = new Map(); this.drag = null; this.pinch = null;
      this.resetView();

      const ro = new ResizeObserver(() => { this.measure(); this.schedule(); });
      ro.observe(canvas);

      canvas.addEventListener("pointermove", (e) => { if (!this.onDrag(e)) this.onPointer(e); });
      canvas.addEventListener("pointerleave", () => { if (!this.drag) this.clearHover(); });
      canvas.addEventListener("keydown", (e) => this.onKey(e));
      canvas.addEventListener("wheel", (e) => this.onWheel(e), { passive: false });
      canvas.addEventListener("pointerdown", (e) => this.onDown(e));
      for (const type of ["pointerup", "pointercancel"]) canvas.addEventListener(type, (e) => this.onUp(e));
      canvas.addEventListener("dblclick", (e) => this.onDouble(e));
    }

    resetView() { this.span = null; this.offset = 0; this.yZoom = 1; }

    local(e) {
      const b = this.canvas.getBoundingClientRect();
      return { x: e.clientX - b.left, y: e.clientY - b.top };
    }

    // The view moves by whole and part bars, never past the oldest or the newest.
    setOffset(offset) {
      const slots = this.geom ? this.geom.slots : this.candles.length;
      this.offset = clamp(offset, 0, Math.max(0, this.candles.length - slots));
      if (this.offset < 1e-6) this.offset = 0;
    }

    // Dragged `dx` px: right shows older, left newer.
    panBy(dx) {
      if (!this.geom) return;
      this.setOffset(this.offset + dx / this.geom.stride);
      this.schedule();
    }

    // Zoomed by `factor` (above 1: out) about the point at `x`, which keeps its
    // place; a view following the market zooms about its right edge and keeps following.
    zoomAt(factor, x) {
      const g = this.geom;
      if (!g) return;
      const n = this.candles.length, was = g.slots;
      const r = this.offset === 0 ? 1 : clamp((x - g.left) / g.width, 0, 1);
      let span = Math.round(was * factor);
      if (span === was) span += factor > 1 ? 1 : -1;
      span = clamp(span, Math.min(MIN_BARS, n), n);
      // bars right of the point: (1 - r) of the view before, (1 - r) of it after
      const right = this.offset + was * (1 - r);
      this.span = span >= n ? null : span;
      this.offset = clamp(right - span * (1 - r), 0, n - span);
      if (this.offset < 1e-6) this.offset = 0;
      this.schedule();
    }

    onWheel(e) {
      if (!this.geom) return;
      e.preventDefault();
      const unit = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? this.geom.width : 1;
      if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) this.panBy(-e.deltaX * unit);
      else if (e.deltaY) this.zoomAt(Math.exp(e.deltaY * unit * 0.002), this.local(e).x);
    }

    onDown(e) {
      if (e.button !== 0 || !this.geom) return;
      this.canvas.setPointerCapture?.(e.pointerId);
      this.pointers.set(e.pointerId, this.local(e));
      if (this.pointers.size === 2) {           // a second finger: pinch
        const [a, b] = [...this.pointers.values()];
        this.pinch = Math.hypot(a.x - b.x, a.y - b.y);
        this.drag = null;
        return;
      }
      const { x, y } = this.local(e);
      this.drag = { x, y, last: x, gutter: x > this.geom.left + this.geom.width, yZoom: this.yZoom, moved: false };
    }

    // True while a drag or pinch owns the pointer, so the readout holds still.
    onDrag(e) {
      if (!this.pointers.has(e.pointerId)) return false;
      const p = this.local(e);
      this.pointers.set(e.pointerId, p);
      if (this.pinch && this.pointers.size === 2) {
        const [a, b] = [...this.pointers.values()];
        const dist = Math.hypot(a.x - b.x, a.y - b.y);
        if (dist > 0) this.zoomAt(this.pinch / dist, (a.x + b.x) / 2);
        this.pinch = dist;
        return true;
      }
      const d = this.drag;
      if (!d) return false;
      if (!d.moved && Math.abs(p.x - d.x) < 3 && Math.abs(p.y - d.y) < 3) return false;   // a click, not a drag
      if (!d.moved) { d.moved = true; this.clearHover(); }
      this.canvas.style.cursor = d.gutter ? "ns-resize" : "grabbing";
      if (d.gutter) {               // the price scale: drag down to squeeze the candles, up to stretch
        this.yZoom = clamp(d.yZoom * Math.exp((p.y - d.y) / 200), 0.05, 20);
        this.schedule();
      } else {
        this.panBy(p.x - d.last);
        d.last = p.x;
      }
      return true;
    }

    onUp(e) {
      if (!this.pointers.delete(e.pointerId)) return;
      if (this.pointers.size < 2) this.pinch = null;
      if (this.pointers.size) return;
      this.drag = null;
      this.canvas.style.cursor = "";
      // released outside the chart: the capture swallowed its pointerleave
      const { x, y } = this.local(e);
      if (x < 0 || y < 0 || x > this.canvas.clientWidth || y > this.canvas.clientHeight) this.clearHover();
    }

    // Double-click: the price scale goes back to fitting the candles; anywhere else the whole view resets.
    onDouble(e) {
      if (!this.geom) return;
      if (this.local(e).x > this.geom.left + this.geom.width) this.yZoom = 1;
      else this.resetView();
      this.schedule();
    }

    measure() {
      const narrow = this.canvas.clientWidth < 620;
      this.inset = { top: 20, right: narrow ? 66 : 78, bottom: 26, left: 14 };
      this.axisGuard = this.inset.left;
    }

    // Another index or session opens on the newest bars; a refresh keeps a
    // view scrolled back on the bars it showed as new ones arrive, and the bar
    // being read out.
    setData(candles, pivots) {
      const old = this.candles;
      const fresh = !old.length || !candles.length || old[0].timestamp !== candles[0].timestamp;
      // a new series voids the drawn geometry until the next frame draws it
      if (fresh) { this.resetView(); this.geom = null; }
      else if (this.offset > 0) this.offset += candles.length - old.length;
      this.candles = candles;
      this.pivots = [...pivots.hh, ...pivots.ll];
      if (fresh || this.hover === null || this.hover >= candles.length) {
        this.hover = null; this.hoverY = null;
        this.tip.hidden = true;
      }
      this.schedule(); this.emit(this.hover === null ? null : candles[this.hover]);
    }

    emit(candle = null) {
      if (this.onSelect) this.onSelect(candle || this.candles[this.candles.length - 1] || null, candle !== null);
    }

    schedule() {
      if (this.frame !== null) return;
      this.frame = requestAnimationFrame(() => { this.frame = null; this.render(); });
    }

    clearHover() {
      this.hover = null; this.hoverY = null;
      this.tip.hidden = true;
      this.emit(); this.schedule();
    }

    onPointer(e) {
      if (!this.geom || this.pinch) return;
      const { x, y } = this.local(e);
      const { left, top, width, height, stride, start, end, edge, min, max } = this.geom;
      if (!this.drag) this.canvas.style.cursor = x > left + width ? "ns-resize" : "";
      if (x < left || x > left + width || y < top || y > top + height) return this.clearHover();
      // `hover` indexes the whole series; the view holds bars `start`..`end`.
      const i = clamp(start + Math.floor(end - start - (edge - x) / stride), 0, Math.min(end, this.candles.length) - 1);
      this.hover = i; this.hoverY = y;
      this.emit(this.candles[i]);
      this.showTip(e.clientX, e.clientY, this.candles[i], max - ((y - top) / height) * (max - min));
      this.schedule();
    }

    // The arrows walk the readout across the bars, panning at either edge;
    // + and - zoom, End returns to the newest bars.
    onKey(e) {
      const n = this.candles.length;
      if (!n || !this.geom) return;
      if (["+", "=", "-", "_"].includes(e.key)) {
        e.preventDefault();
        return this.zoomAt(e.key === "+" || e.key === "=" ? 0.8 : 1.25, this.geom.left + this.geom.width / 2);
      }
      if (e.key === "End") { e.preventDefault(); this.resetView(); return this.schedule(); }
      if (!["ArrowLeft", "ArrowRight", "Escape"].includes(e.key)) return;
      e.preventDefault();
      if (e.key === "Escape") return this.clearHover();
      const slots = this.geom.slots;
      const from = this.hover === null ? n - 1 - Math.ceil(this.offset) : this.hover;
      this.hover = Math.max(0, Math.min(n - 1, from + (e.key === "ArrowRight" ? 1 : -1)));
      // a bar off the view, or cut by its edge, is panned to whole
      if (this.hover < n - slots - this.offset) this.setOffset(n - slots - this.hover);
      else if (this.hover > n - 1 - this.offset) this.setOffset(n - 1 - this.hover);
      this.hoverY = null;
      this.tip.hidden = true;
      this.emit(this.candles[this.hover]);
      this.schedule();
    }

    // The tip is fixed-position, so it is placed in viewport coordinates and
    // works the same whether the canvas is full-bleed or a card in a stack.
    showTip(clientX, clientY, candle, price) {
      $("tipTime").textContent = fTime(candle.timestamp, true);
      $("tipPrice").textContent = fmt(price);
      this.tip.hidden = false;
      this.tip.style.left = `${Math.max(8, Math.min(window.innerWidth - 130, clientX + 14))}px`;
      this.tip.style.top = `${Math.max(8, Math.min(window.innerHeight - 58, clientY - 24))}px`;
    }

    round(x, y, w, h, r) {
      const c = this.ctx, rr = Math.min(r, w / 2, h / 2);
      c.beginPath();
      c.moveTo(x + rr, y);
      c.arcTo(x + w, y, x + w, y + h, rr);
      c.arcTo(x + w, y + h, x, y + h, rr);
      c.arcTo(x, y + h, x, y, rr);
      c.arcTo(x, y, x + w, y, rr);
      c.closePath();
    }

    drawPivot(x, y, pivot, top, bottom) {
      const c = this.ctx;
      const high = pivot.type === "HH";
      const colour = high ? P.down : P.up;
      const tipY = high ? y - 5 : y + 5;
      const base = high ? y - 13 : y + 13;

      c.save();
      c.fillStyle = colour;
      c.shadowColor = high ? P.downLine : P.upLine;
      c.shadowBlur = 8;
      c.beginPath();
      c.moveTo(x, tipY); c.lineTo(x - 5, base); c.lineTo(x + 5, base); c.closePath();
      c.fill();
      c.shadowBlur = 0;

      const w = 30, h = 18, gap = 5;
      // The badge sits outside the arrow, away from the candle. Where the plot
      // is too short for that side it flips rather than sliding back across its
      // own arrow — clamping alone drew the badge on top of the marker whenever
      // a pivot landed near the top of a short chart.
      const outside = high ? base - gap - h : base + gap;
      const flipped = high ? tipY + gap : tipY - gap - h;
      const fits = outside >= top && outside + h <= bottom;
      const by = Math.max(top + 1, Math.min(bottom - h - 1, fits ? outside : flipped));

      this.round(x - w / 2, by, w, h, 6);
      c.fillStyle = high ? P.downSoft : P.upSoft;
      c.fill();
      c.strokeStyle = high ? P.downLine : P.upLine;
      c.lineWidth = 1;
      c.stroke();
      c.fillStyle = colour;
      c.font = "700 9px ui-sans-serif, system-ui";
      c.textAlign = "center";
      c.textBaseline = "middle";
      c.fillText(high ? "SELL" : "BUY", x, by + h / 2 + 0.5);
      c.restore();
    }

    render() {
      const c = this.ctx;
      if (!c) return;
      const W = this.canvas.clientWidth, H = this.canvas.clientHeight;
      if (W < 1 || H < 1) return;

      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const tw = Math.round(W * dpr), th = Math.round(H * dpr);
      if (this.canvas.width !== tw || this.canvas.height !== th) { this.canvas.width = tw; this.canvas.height = th; }
      c.setTransform(dpr, 0, 0, dpr, 0, 0);
      c.clearRect(0, 0, W, H);

      const all = this.candles, n = all.length;
      if (!n) { this.geom = null; return; }

      const { top, right, bottom, left } = this.inset;
      const width = Math.max(1, W - left - right);
      // The time axis is drawn below the plot, so its own row has to come out
      // of the height before anything is scaled — otherwise the labels land
      // exactly where the dock begins and print through the glass.
      const height = Math.max(1, H - top - bottom - AXIS_ROW);

      // What is in view: whole bars from the right edge, plus the one cut by
      // the left edge while the view sits between bars.
      const slots = clamp(this.span ?? n, 1, n);
      this.offset = clamp(this.offset, 0, n - slots);
      const whole = Math.floor(this.offset), part = this.offset - whole;
      const end = n - whole, start = Math.max(0, end - slots - (part > 0 ? 1 : 0));
      const candles = all.slice(start, end), count = candles.length;
      const stride = width / slots;
      const edge = left + width + part * stride;

      const first = candles[0].timestamp, last = candles[count - 1].timestamp;
      const shown = this.pivots.filter((p) => p.timestamp >= first && p.timestamp <= last);
      const values = candles.flatMap((b) => [b.high, b.low]).concat(shown.map((p) => p.price));
      let min = Math.min(...values), max = Math.max(...values);
      const pad = Math.max((max - min) * 0.085, max * 0.0005, 1);
      min -= pad; max += pad;
      if (this.yZoom !== 1) {
        const mid = (min + max) / 2, half = (max - min) / 2 * this.yZoom;
        min = mid - half; max = mid + half;
      }
      const span = max - min || 1;
      const bodyW = Math.max(2, Math.min(11, stride * 0.62));
      const xAt = (i) => edge - (count - i - 0.5) * stride;
      const yAt = (p) => top + ((max - p) / span) * height;

      this.geom = { left, top, width, height, stride, slots, start, end, edge, min, max };

      const narrow = W < 620;
      c.save();
      c.font = `${narrow ? 9.5 : 10.5}px ${P.mono}`;
      c.lineWidth = 1;
      c.textBaseline = "middle";

      // The last-price badge is painted over this gutter below. A scale label
      // landing under it is unreadable and says less than the badge does, so
      // the gridline still draws and only the number is skipped.
      // The last price is the newest bar's close wherever the view is; panned
      // or scaled off the plot, its line and badge are not drawn.
      const latest = all[n - 1];
      const lastShown = latest.close >= min && latest.close <= max;
      const lastY = lastShown ? yAt(latest.close) : -1e9;
      const rows = height < 340 ? 4 : 6;
      for (let i = 0; i <= rows; i += 1) {
        const ratio = i / rows;
        const y = top + ratio * height;
        c.beginPath();
        c.strokeStyle = P.grid;
        c.moveTo(left, Math.round(y) + 0.5);
        c.lineTo(left + width, Math.round(y) + 0.5);
        c.stroke();
        if (Math.abs(y - lastY) < 13) continue;
        c.fillStyle = P.axis;
        c.textAlign = "right";
        c.fillText(fmt(max - ratio * span), W - 12, y);
      }

      // Labelled by the bar's place in the whole series, so labels move with
      // the bars as they pan; a label that would not print whole is skipped.
      const ticks = width < 420 ? 3 : width < 820 ? 5 : 8;
      const every = Math.max(1, Math.ceil(slots / ticks));
      for (let idx = 0; idx < count; idx += 1) {
        if ((start + idx) % every) continue;
        const x = xAt(idx);
        const label = fTime(candles[idx].timestamp);
        const half = c.measureText(label).width / 2;
        if (x - half < this.axisGuard || x + half > left + width) continue;
        c.beginPath();
        c.strokeStyle = P.gridSoft;
        c.moveTo(Math.round(x) + 0.5, top);
        c.lineTo(Math.round(x) + 0.5, top + height);
        c.stroke();
        c.fillStyle = P.axis;
        c.textAlign = "center";
        c.fillText(label, x, top + height + 12);
      }

      // The plot is clipped to its box: a bar half off either edge is cut there.
      c.save();
      c.beginPath(); c.rect(left, 0, width, H); c.clip();
      candles.forEach((k, i) => {
        const x = xAt(i), rising = k.close >= k.open;
        c.beginPath();
        c.strokeStyle = rising ? P.up : P.down;
        c.lineWidth = 1;
        c.moveTo(Math.round(x) + 0.5, yAt(k.high));
        c.lineTo(Math.round(x) + 0.5, yAt(k.low));
        c.stroke();
        const oy = yAt(k.open), cy = yAt(k.close);
        c.fillStyle = rising ? P.upFill : P.downFill;
        c.fillRect(x - bodyW / 2, Math.min(oy, cy), bodyW, Math.max(1.5, Math.abs(cy - oy)));
      });

      shown.forEach((pivot) => {
        let best = 0, bestD = Infinity;
        candles.forEach((k, i) => {
          const d = Math.abs(k.timestamp - pivot.timestamp);
          if (d < bestD) { bestD = d; best = i; }
        });
        this.drawPivot(xAt(best), yAt(pivot.price), pivot, top, top + height);
      });
      c.restore();

      const rising = latest.close >= latest.open;
      if (lastShown) {
        c.save();
        c.beginPath();
        c.setLineDash([3, 5]);
        c.strokeStyle = rising ? P.upLine : P.downLine;
        c.moveTo(left, lastY); c.lineTo(left + width, lastY);
        c.stroke();
        c.setLineDash([]);
        const bw = right - 14, bh = 20;
        this.round(W - right + 8, lastY - bh / 2, bw, bh, 6);
        c.fillStyle = rising ? P.up : P.down;
        c.shadowColor = rising ? P.upLine : P.downLine;
        c.shadowBlur = 12;
        c.fill();
        c.shadowBlur = 0;
        c.fillStyle = P.badgeInk;
        c.font = `700 ${narrow ? 9.5 : 10}px ${P.mono}`;
        c.textAlign = "center";
        c.textBaseline = "middle";
        c.fillText(fmt(latest.close), W - right + 8 + bw / 2, lastY + 0.5);
        c.restore();
      }

      if (this.hover !== null && this.hover >= start && this.hover < end) {
        const hx = xAt(this.hover - start);
        const hy = this.hoverY ?? yAt(all[this.hover].close);
        c.save();
        c.beginPath();
        c.setLineDash([3, 5]);
        c.strokeStyle = P.cross;
        c.moveTo(hx, top); c.lineTo(hx, top + height);
        c.moveTo(left, hy); c.lineTo(left + width, hy);
        c.stroke();
        c.setLineDash([]);
        c.beginPath();
        c.fillStyle = P.crossDot;
        c.arc(hx, yAt(all[this.hover].close), 3, 0, Math.PI * 2);
        c.fill();
        c.restore();
      }

      c.restore();
    }
  }

  const plot = $("plot");

  const chart = new Chart(plot, $("tip"));

  chart.onSelect = (candle, hovered) => {
    const values = candle ? [candle.open, candle.high, candle.low, candle.close].map(fmt) : ["—", "—", "—", "—"];
    [el.o, el.h, el.l, el.c].forEach((cell, i) => { cell.textContent = values[i]; });
    el.c.classList.remove("up", "down");
    if (candle) el.c.classList.add(candle.close >= candle.open ? "up" : "down");
    el.candleAt.textContent = !candle ? "Candle" : `${hovered ? "" : "Latest · "}${fTime(candle.timestamp)}`;
  };

  // ── State ────────────────────────────────────────────────────────────────
  let candles = [];
  let sessionOpen = null;
  let hasTick = false, lastTick = null;
  let feedErrors = 0;

  function setFeed(state, label) { el.feedPod.dataset.state = state; el.feedLabel.textContent = label; }

  function setVeil(state, title, detail) {
    el.veil.dataset.state = state;
    el.veil.querySelector("b").textContent = title;
    el.veil.querySelector("small").textContent = detail;
    el.veil.classList.toggle("is-gone", state === "hidden");
  }

  function setPrice(value) {
    if (!Number.isFinite(value)) { el.priceMain.textContent = "—"; el.pricePaise.textContent = ""; return; }
    const text = nf.format(value), cut = text.lastIndexOf(".");
    el.priceMain.textContent = cut === -1 ? text : text.slice(0, cut);
    el.pricePaise.textContent = cut === -1 ? "" : text.slice(cut);
  }


  // Clamped, because a live tick can print outside the last completed candle's
  // range and a pin sliding off its own track reads as a bug, not a new high.

  function tickClock() {
    const now = new Date();
    el.clock.textContent = fClock.format(now);
  }

  const uptime = (s) => {
    if (!Number.isFinite(s)) return null;
    const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
    return d ? `up ${d}d ${h}h` : h ? `up ${h}h ${m}m` : `up ${m}m`;
  };

  // ── Health ───────────────────────────────────────────────────────────────
  // The Machine tag summarises three sources, so each files its own verdict and
  // the tag reports the worst. A broker that had gone dark must never be able
  // to sit underneath the words "All normal".
  const health = { broker: "unknown", bot: "unknown", db: "unknown", ws: "unknown" };
  const RANK = { healthy: 0, unknown: 1, warning: 2, error: 3 };
  const LABEL = { healthy: "All normal", unknown: "Checking", warning: "Attention", error: "Attention" };
  let healthNote = null;

  function updateHealth() {
    const worst = Object.values(health).reduce((a, x) => (RANK[x] > RANK[a] ? x : a), "healthy");
    el.healthTag.textContent = healthNote || LABEL[worst];
    el.healthTag.dataset.health = worst === "healthy" ? "healthy" : worst === "unknown" ? "unknown" : "warning";
  }

  const cpuHealth = (cpu, up = true) =>
    !up ? "error" : !Number.isFinite(cpu) ? "unknown" : cpu >= 85 ? "warning" : "healthy";

  // ── Broker latency ───────────────────────────────────────────────────────
  // Anchored to what this deployment actually measures: Shoonya answers from
  // AWS ap-south-1 in Mumbai and this host reaches it in ~11-15 ms, TLS
  // handshake included, because shoonya_api opens a fresh connection per call.
  // A generic "250 ms is fine" ladder would peg the meter at four bars for the
  // life of the console and never move; an instrument that cannot tell a good
  // day from a bad one is decoration. One list, not two, so the ladder and the
  // colour can never disagree.
  const TIERS = [
    { max: 25, quality: "good", level: 4 },
    { max: 75, quality: "good", level: 3 },
    { max: 250, quality: "fair", level: 2 },
    { max: Infinity, quality: "poor", level: 1 },
  ];
  const DEAD = { quality: "down", level: 0 };
  const PING_HEALTH = { good: "healthy", fair: "warning", poor: "warning", down: "error" };
  const SPARK_N = 24, SPARK_MEDIAN_HEADROOM = 2.5, SPARK_PEAK_HEADROOM = 1.05, SPARK_MIN_CEIL = 15;

  const tier = (ms) => (Number.isFinite(ms) ? TIERS.find((t) => ms <= t.max) : DEAD);
  const fmtMs = (ms) => (Number.isFinite(ms) ? `${Math.round(ms)} ms` : "—");

  // Not num(): that goes through Number(), and Number(null) is 0 — which would
  // render an unreachable broker as a flawless "0 ms". Here the absence of a
  // measurement is the measurement.
  const millis = (x) => (x === null || x === undefined ? null : num(x));

  function median(xs) {
    if (!xs.length) return null;
    const s = [...xs].sort((a, b) => a - b), m = s.length >> 1;
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  }

  function renderSpark(history) {
    const samples = Array.isArray(history) ? history.slice(-SPARK_N) : [];
    const ok = samples.filter((x) => Number.isFinite(x));
    // Anchored to the window's own median so a calm link draws a legible ripple
    // at about a third height rather than a flat line on the floor; the peak
    // term keeps the tallest sample inside the box, so one bad probe rescales
    // the row and stands out instead of being clipped into it.
    const ceil = Math.max(SPARK_MIN_CEIL, (median(ok) || 0) * SPARK_MEDIAN_HEADROOM, Math.max(0, ...ok) * SPARK_PEAK_HEADROOM);
    el.spark.replaceChildren(...samples.map((x) => {
      const bar = document.createElement("i");
      bar.dataset.q = tier(x).quality;
      // A probe that never answered fills the column; every real sample keeps a
      // floor so a fast link still draws a line, not a gap.
      bar.style.height = Number.isFinite(x) ? `${Math.min(100, Math.max(12, Math.round((x / ceil) * 100)))}%` : "100%";
      return bar;
    }));
  }

  function applyPing(data) {
    // No probe at all (no broker session, route down, request failed) is not a
    // slow link — it is an unknown one, and must not be painted red as though
    // the broker had refused us.
    if (!data || data.available === false) {
      el.pingPod.dataset.quality = "unknown";
      el.pingLadder.dataset.level = "0";
      el.pingValue.textContent = "—";
      el.pingPod.title = "Round-trip time to the Shoonya API";
      el.brokerPing.textContent = "—";
      el.brokerMeta.textContent = data?.reason || "Latency probe unavailable";
      el.brokerLed.dataset.health = "unknown";
      el.spark.replaceChildren();
      health.broker = "unknown";
      return updateHealth();
    }

    const latency = millis(data.latency_ms);
    const med = millis(data.median_ms);
    const peak = millis(data.peak_ms);
    const failures = Number(data.failures) || 0;
    const t = tier(latency);

    el.pingPod.dataset.quality = t.quality;
    el.pingLadder.dataset.level = String(t.level);
    el.pingValue.textContent = latency === null ? "offline" : fmtMs(latency);
    el.pingPod.title = latency === null
      ? `Shoonya did not answer the last probe${data.error ? ` (${data.error})` : ""}`
      : `Round-trip time to the Shoonya API · median ${fmtMs(med)}`;

    el.brokerPing.textContent = latency === null ? "No answer" : fmtMs(latency);
    el.brokerMeta.textContent = [
      med === null ? null : `median ${fmtMs(med)}`,
      peak === null ? null : `peak ${fmtMs(peak)}`,
      failures ? `${failures}/${data.samples} failed` : `${data.samples} probes`,
    ].filter(Boolean).join(" · ");

    // The pod reports the last sample, because "what is it now" is the question
    // a pod answers. The verdict does not: with bands this tight a single
    // hiccup would flip the whole bay to "Attention" and back a poll later, and
    // a panel that cries wolf stops being read. A sustained problem moves the
    // median. A probe that got no answer is never averaged away — right now, on
    // this poll, the broker is not responding.
    health.broker = latency === null ? "error" : PING_HEALTH[tier(med ?? latency).quality];
    el.brokerLed.dataset.health = health.broker;
    renderSpark(data.history);
    updateHealth();
  }

  // ── Pollers ──────────────────────────────────────────────────────────────
  async function refreshSnapshot() {
    try {
      const snap = await fetchJson(forIndex("/realtime_pivot_snapshot"));
      const rows = normalizeCandles(snap?.candles);
      const raw = snap?.pivots || {};
      const pivots = { hh: normalizePivots(raw.hh_points, "HH"), ll: normalizePivots(raw.ll_points, "LL") };

      candles = rows;
      chart.setData(rows, pivots);
      updateMarket(rows);
      el.barCount.textContent = `${rows.length} bars`;

      feedErrors = 0;
      setFeed("live", "Live");
      if (rows.length) setVeil("hidden", "", "");
      else setVeil("waiting", "Waiting for the first candle", `The ${timeframe} feed is connected and will populate automatically.`);
    } catch (_e) {
      feedErrors += 1;
      setFeed("error", "Delayed");
      if (!candles.length) setVeil("error", "Market feed unavailable", "The console will reconnect automatically.");
      if (feedErrors === 3) toast("Market feed delayed — reconnecting automatically.");
    }
  }

  function updateMarket(rows) {
    const first = rows[0], latest = rows[rows.length - 1];
    sessionOpen = first ? first.open : null;

    if (!latest) {
      if (!hasTick) { setPrice(null); el.delta.textContent = "Waiting for candles"; }
      return;
    }

    // Once the live tick feed has spoken it owns price, delta and as-of;
    // candles only refine the session open above. Before that, candle
    // close is the best price available, so it shows (and flashes) as a
    // fallback.
    if (hasTick) return;
    const diff = latest.close - first.open;
    setPrice(latest.close);
    el.delta.textContent = `${signed(diff)} · ${signed(first.open ? (diff / first.open) * 100 : 0, "%")}`;
    tone(el.delta, diff);
    el.asOf.textContent = `${fTime(latest.timestamp)} IST`;
    lastTick = latest.close;
  }

  function applyPrice(price) {
    hasTick = true;
    setPrice(price);
    if (sessionOpen) {
      const diff = price - sessionOpen;
      el.delta.textContent = `${signed(diff)} · ${signed((diff / sessionOpen) * 100, "%")}`;
      tone(el.delta, diff);
    }
    el.asOf.textContent = `${fClock.format(new Date())} IST · live`;
    lastTick = price;
  }

  // Every tick, pushed: the server writes each new LTP the moment the feed
  // prints it, so nothing here samples or caps the rate. A dropped stream is
  // silent — the last price stays up and EventSource reconnects by itself.
  let priceStream = null;
  function openPriceStream() {
    if (priceStream) priceStream.close();
    priceStream = null;
    const stream = priceStream = new EventSource(forIndex("/live_price_stream"));
    stream.onmessage = (event) => {
      const price = num(event.data);
      // A message from a stream already replaced by an index switch is the
      // old instrument's price.
      if (stream === priceStream && price !== null) applyPrice(price);
    };
  }

  // The whole bay for whichever index it is pointed at. It reads from
  // `/entry_gates` rather than from the chart's own snapshot, because the index
  // it reports is not necessarily the one being charted.
  function renderEngine() {
    const engine = engines[engineIndex] || null;
    const last = engine && engine.last;
    const stamp = parseFeedTime(last && last.time);
    const price = num(last && last.price);

    // The bar still forming, scored against the tick, is the reading an
    // operator is actually asking for: "am I in or out right now", not "what
    // did the bar five minutes ago decide". It is a forecast — v3 acts on
    // closes and nothing else — so it is worded as one, and the settled verdict
    // takes over the moment the tick goes stale or the session ends.
    const signal = (engine && (engine.live || engine.signal)) || null;

    if (!last || !Number.isFinite(stamp) || price === null) {
      el.engineBay.dataset.signal = "none";
      el.bias.textContent = "No signal";
      el.biasMeta.textContent = engineMeta(signal);
    } else {
      const high = last.type === "HH";
      el.engineBay.dataset.signal = high ? "short" : "long";
      el.bias.textContent = high ? "SHORT bias" : "LONG bias";
      el.biasMeta.textContent = `Entry at ${fmt(price)} · ${fTime(stamp)} IST`;
    }
    // The gates render either way: v3 scores every bar and takes a trade on
    // very few of them, so "which gate is holding this bar out" is the reading
    // an operator needs on the ~99% of bars that produce no marker at all.
    updateGates(signal);
    updateCarry(signal, last);
    updateReach();
  }

  // Every index's distance to its next signal, under its chip: the points the
  // close still has to climb before every price gate clears. "✓" is at or
  // above that level, "off" is a day no price can signal on (the ATR is below
  // cost, B on a shock session, or past the carry window).
  function updateReach() {
    if (!el.enginePicker) return;
    for (const chip of el.enginePicker.querySelectorAll(".picker-chip")) {
      const name = chip.dataset.index;
      const engine = engines[name];
      const r = (engine && (engine.live || engine.signal) || {}).reach;
      const tag = chip.querySelector(".reach");
      const pts = num(r && r.points);
      if (!tag) continue;
      if (!r || pts === null) {
        tag.textContent = ""; tag.removeAttribute("data-state");
        chip.title = name; chip.setAttribute("aria-label", name);
        continue;
      }
      const state = !r.open ? "off" : pts <= 0 ? "in" : num(r.atr) <= 0.25 ? "near" : "far";
      tag.dataset.state = state;
      tag.textContent = state === "off" ? "off" : state === "in" ? "✓"
        : `+${pts < 1000 ? Math.round(pts) : `${(pts / 1000).toFixed(1)}k`}`;
      const note = state === "off" ? `no ${r.leg} signal possible today`
        : `${r.leg} ${state === "in" ? "clear of" : `needs ${signed(pts)} pts (${r.atr} ATR) to`} ${fmt(num(r.price))}`;
      chip.title = `${name} · ${note}`;
      chip.setAttribute("aria-label", `${name}, ${note}`);
    }
  }

  // Every gate by name, pass or fail. "Which gate stopped this trade" is the
  // whole question this bay exists to answer, and a tooltip cannot be opened on
  // a phone.
  function updateGates(signal) {
    const gates = signal && Array.isArray(signal.gates) ? signal.gates : null;
    if (!gates || !gates.length) {
      el.gates.hidden = true;
      el.gates.replaceChildren();
      el.verdict.textContent = "Idle";
      el.verdict.removeAttribute("data-verdict");
      el.verdict.removeAttribute("data-live");
      return;
    }

    const passed = gates.filter((g) => g && g.ok).length;
    const clear = passed === gates.length;
    // Three words, because the gates and the trade are not the same question.
    // "Accepted" is v3 taking the bar. "Clear" is every gate passing without
    // that: a bar still forming has decided nothing yet, and even a closed one
    // can pass all four and not be traded — the session's first qualifying bar
    // takes the leg, and only if this index wins the slot. "Rejected 4/4" is
    // the reading that sentence has to stop the bay from ever showing.
    const word = signal.accepted === true ? "Accepted" : clear ? "Clear" : "Blocked";
    el.verdict.textContent = `${word} ${passed}/${gates.length}`;
    el.verdict.dataset.verdict = clear ? "accepted" : "rejected";
    if (signal.live) el.verdict.dataset.live = "yes";
    else el.verdict.removeAttribute("data-live");

    el.gates.hidden = false;
    el.gates.replaceChildren(...gates.map((gate) => {
      const row = document.createElement("li");
      row.dataset.ok = gate && gate.ok ? "yes" : "no";
      const name = document.createElement("span");
      // Gate names keep their own casing — lowercasing turns "ATR vs cost" into
      // "atr vs cost".
      name.textContent = gate?.name || "—";
      const mark = document.createElement("b");
      mark.textContent = gate && gate.ok ? "PASS" : "FAIL";
      row.append(name, mark);
      return row;
    }));
  }

  // What the gate rows below are actually being scored on, so a moving reading
  // is never mistaken for a decided one — and so a settled bay says which bar
  // it has stopped at rather than looking live and stale at once.
  function engineMeta(signal) {
    if (!signal) return "Waiting for an entry signal";
    const at = fTime(parseFeedTime(signal.confirmed_time));
    const bar = at === "—" ? "" : ` ${at}`;
    const r = signal.reach;
    const pts = num(r && r.points);
    const reach = !r || !r.open || pts === null ? ""
      : pts > 0 ? ` · ${signed(pts)} to ${fmt(num(r.price))}` : ` · clear of ${fmt(num(r.price))}`;
    if (!signal.live) return `Last close${bar || " —"} IST${reach}`;
    const price = num(signal.price);
    return `Live${bar && ` on the${bar} bar`}${price === null ? "" : ` · ${fmt(price)}`}${reach}`;
  }

  // The overnight leg, as one line. It shares the three gates above with the
  // entry and differs only in waiting for the 14:50 bar instead of for a move,
  // so what it has to add is the state of that wait: still clear, stopped by a
  // gate you can already see failing above, taken, or settled for the day.
  function updateCarry(signal, last) {
    const carry = signal && signal.carry;
    if (!carry) {
      el.carry.hidden = true;
      return;
    }

    const blocked = Array.isArray(carry.blocked_by) ? carry.blocked_by : [];
    // A carry that was taken is read off today's marker, not off the gates: by
    // the time the fill happens the bar being scored has moved past 14:50 and
    // the gates no longer remember the night they let through.
    const state = (carry.accepted || (last && last.leg === "carry")) ? "taken"
      : blocked.length ? "blocked"
      : signal.tod > CARRY_TOD ? "closed"
      : "clear";

    el.carry.hidden = false;
    el.carry.dataset.state = state;
    el.carryNote.textContent = {
      taken: "Taken · night trade fills at 15:11",
      // Before 14:50 these gates are a forecast of what that bar will
      // find, which is the whole point of watching them all day.
      // The failing gates are named in the rows directly above, so this counts
      // them rather than repeating them into an ellipsis.
      blocked: blocked.length === 1 ? `Blocked · ${blocked[0]}`
        : `Blocked · ${blocked.length} gates above`,
      closed: "Window closed · no carry tonight",
      clear: "Clear · would fill at 15:11",
    }[state];
    el.carryScore.textContent = `${carry.passed}/${carry.total}`;
  }

  // ── Books: DAY (the HH/LL desk) and NIGHT (v3) ───────────────────────────
  // One read of `/books` feeds the session bay, today's trades and the
  // account, so the three can never disagree. A book the server could
  // not read arrives as null and is shown as unknown, never as flat: a ledger
  // that did not answer is not a flat book, and only one of the two means it
  // is safe to walk away.
  const store = {
    get(key) { try { return window.localStorage.getItem(key); } catch (_e) { return null; } },
    set(key, value) { try { window.localStorage.setItem(key, value); } catch (_e) { /* private mode */ } },
  };
  const nf0 = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 });
  const fIstDay = new Intl.DateTimeFormat("en-CA", { ...IST, year: "numeric", month: "2-digit", day: "2-digit" });
  const rupees = (x) => (Number.isFinite(x) ? `${x > 0 ? "+" : x < 0 ? "−" : ""}₹${nf0.format(Math.abs(x))}` : "—");
  const BOOK_NAME = { DAY: "Day · HH/LL desk, intraday", NIGHT: "Night · v3, held overnight" };
  const BOOK_FILTERS = ["ALL", "DAY", "NIGHT"];
  let books;                      // the last `/books` answer: undefined before the first, null if unreadable
  let bookFilter = BOOK_FILTERS.includes(store.get("lune.books")) ? store.get("lune.books") : "ALL";

  async function refreshBooks() {
    try { books = await fetchJson("/books", 6000); }
    catch (_e) { books = null; }
    renderSession(); renderBlotter(); renderAccount();
    // The HH/LL tick chart (tick-chart.js) draws the day position's entry line from this read.
    document.dispatchEvent(new CustomEvent("lune:books", { detail: books }));
  }

  function renderSession() {
    const b = books, day = b?.day || null, night = b?.night || null;
    const mode = String(b?.mode || "").toUpperCase();
    if (mode) {
      el.modeValue.textContent = mode;
      el.modePod.dataset.mode = mode === "LIVE" ? "live" : mode === "TEST" ? "test" : "unknown";
    }
    renderHold("day", day); renderHold("night", night);
    // The header: the open trade's unrealised, and each book's realised today.
    const today = (id, value) => {
      const node = $(id);
      node.className = "tnum";
      node.textContent = value === null ? "—" : rupees(value);
      if (value !== null) tone(node, value);
    };
    today("todayOpen", day && night && (day.position || night.position) ? millis(b.open_pnl)
      : day && night ? 0 : null);
    today("todayNight", night ? millis(night.net_pnl) : null);
    today("todayDay", day ? millis(day.net_pnl) : null);
    if (!day && !night) {
      el.pnl.textContent = "—";
      el.pnl.className = "bay-lead tnum flat";
      el.pnlMeta.textContent = "Ledger unavailable";
      el.closed.textContent = "—";
      return;
    }
    // Realised after charges, plus the open positions at their mark.
    const net = millis(b.net_pnl) ?? 0, open = millis(b.open_pnl);
    const held = Boolean(day?.position || night?.position);
    const total = net + (open ?? 0);
    el.pnl.textContent = rupees(total);
    tone(el.pnl, total);
    el.pnlMeta.textContent = !day ? "Day ledger unavailable · night book only"
      : !night ? "Night ledger unavailable · day book only"
      : held ? `Realised ${rupees(net)} · open ${open === null ? "unmarked" : rupees(open)}`
      : b.closed ? "Realised today, after charges" : "No executed trades today";
    el.closed.textContent = `${b.closed} closed`;
  }

  function renderHold(book, data) {
    const row = $(`${book}Hold`);
    const side = row.querySelector(".hold-side"), entry = row.querySelector(".hold-entry");
    const open = row.querySelector("b.hold-open");
    open.className = "hold-open tnum";
    open.removeAttribute("title");
    const p = data?.position || null;
    if (!data) {
      row.dataset.side = "unknown";
      side.textContent = "Unknown"; entry.textContent = "Ledger unavailable"; open.textContent = "—";
      return;
    }
    if (!p) {
      row.dataset.side = "flat";
      side.textContent = "Flat"; entry.textContent = "No open position"; open.textContent = "₹0";
      tone(open, 0);
      return;
    }
    const short = p.direction === "SHORT";
    row.dataset.side = short ? "short" : "long";
    side.textContent = `${short ? "SHORT" : "LONG"} ${p.index}`;
    entry.textContent = `${p.lots ?? "—"} lot${p.lots === 1 ? "" : "s"} · entry ${fmt(millis(p.entry))}`;
    // Open but unmarked (no tick yet): a dash, never a zero that reads as
    // "this position is costing you nothing".
    const pnl = millis(p.open_pnl);
    open.textContent = pnl === null ? "—" : rupees(pnl);
    if (pnl !== null) {
      tone(open, pnl);
      open.title = `${signed(millis(p.open_points), " pts")} at ${fmt(millis(p.mark))}`;
    }
  }

  // A night trade spans two sessions, so a time not from today carries its date.
  function when(iso) {
    const t = parseFeedTime(iso);
    if (!Number.isFinite(t)) return "";
    const at = new Date(t);
    return fIstDay.format(at) === fIstDay.format(new Date()) ? fHm.format(at) : fDayHm.format(at);
  }

  let blotterKey = null;
  function renderBlotter() {
    const b = books, day = b?.day || null, night = b?.night || null;
    const all = Array.isArray(b?.trades) ? b.trades : [];
    const trades = bookFilter === "ALL" ? all : all.filter((t) => t.book === bookFilter);
    const key = JSON.stringify([trades, bookFilter, b?.net_pnl, b?.charges, b?.closed,
      day && [day.net_pnl, day.closed], night && [night.net_pnl, night.closed]]);
    if (key === blotterKey) return;     // the same rows: the DOM stays as it is under the pointer
    blotterKey = key;

    const any = Boolean(day || night);
    const stat = (id, value) => {
      const node = $(id);
      node.textContent = value === null ? "—" : rupees(value);
      if (value === null) node.className = "tnum"; else tone(node, value);
    };
    stat("blotterNet", any ? millis(b.net_pnl) : null);
    stat("blotterDay", day ? millis(day.net_pnl) : null);
    stat("blotterNight", night ? millis(night.net_pnl) : null);
    $("blotterCharges").textContent = any ? rupees(-millis(b.charges)) : "—";
    $("blotterClosed").textContent = any ? `${b.closed} CLOSED · AFTER CHARGES` : "BOTH BOOKS · AFTER CHARGES";
    $("blotterDayMeta").textContent = day ? `${day.closed} CLOSED · HH/LL DESK` : "HH/LL DESK · UNAVAILABLE";
    $("blotterNightMeta").textContent = night ? `${night.closed} CLOSED · V3` : "V3 · UNAVAILABLE";

    const rows = document.createDocumentFragment();
    for (const trade of trades) {
      const tr = document.createElement("tr");
      const book = trade.book === "NIGHT" ? "night" : "day";
      tr.dataset.book = book;
      const tag = document.createElement("span");
      tag.className = "book-tag"; tag.dataset.book = book;
      tag.textContent = trade.book; tag.title = BOOK_NAME[trade.book] || trade.book;
      const opened = when(trade.opened_ist), closed = when(trade.closed_ist);
      const cells = [
        tag, `${opened || "—"}${closed ? `–${closed}` : ""}`,
        trade.index, trade.direction === "SHORT" ? "SHORT" : "LONG", trade.lots ?? "—", fmt(millis(trade.entry)),
        trade.open ? "OPEN" : fmt(millis(trade.exit)),
        trade.open ? "—" : signed(millis(trade.net_points)),
        trade.open ? "—" : rupees(-millis(trade.charges)),
        trade.open ? "—" : rupees(millis(trade.net_pnl)),
      ];
      cells.forEach((value, i) => {
        const td = document.createElement("td");
        if (value instanceof Node) td.appendChild(value); else td.textContent = value;
        if (trade.open && i === 6) td.className = "open";
        if (!trade.open && i === 7) tone(td, millis(trade.net_points));
        if (!trade.open && i === 9) tone(td, millis(trade.net_pnl));
        tr.appendChild(td);
      });
      rows.appendChild(tr);
    }
    $("blotterRows").replaceChildren(rows);

    const missing = [!day && "Day", !night && "Night"].filter(Boolean);
    const status = !b || missing.length === 2 ? "Ledgers unavailable; retrying."
      : missing.length ? `${missing[0]} ledger unavailable; its trades are not shown.`
      : trades.length ? "" : bookFilter === "ALL" ? "No trades today." : `No ${bookFilter.toLowerCase()} trades today.`;
    $("blotterStatus").textContent = status;
    $("blotterStatus").hidden = !status;
  }

  function selectBookFilter(next) {
    if (!BOOK_FILTERS.includes(next)) return;
    bookFilter = next;
    store.set("lune.books", next);
    for (const button of document.querySelectorAll("#bookFilter [data-book]"))
      button.setAttribute("aria-pressed", String(button.dataset.book === next));
    renderBlotter();
  }

  // ── Account ──────────────────────────────────────────────────────────────
  // Capital + every realised net of both books (all days, after charges); the
  // open trade's unrealised is shown in the header, never in the balance. Null
  // from the server unless both books answered, so a balance missing one of
  // them is never shown. Shoonya's own funds sit beside
  // it, read on their own, slower poll (a broker call).
  let broker;                     // the last `/account_balance` answer: undefined before the first, null if unreadable
  async function refreshBroker() {
    try { broker = await fetchJson("/account_balance", 9000); }
    catch (_e) { broker = null; }
    renderAccount();
  }

  function renderAccount() {
    const a = books === undefined ? undefined : books?.account || null;
    const money = (id, value, signedTone) => {
      const node = $(id);
      node.className = "tnum";
      node.textContent = value === null ? "—" : signedTone ? rupees(value) : `₹${nf0.format(value)}`;
      if (signedTone && value !== null) tone(node, value);
    };
    const paper = String(books?.mode || body.dataset.mode || "").toUpperCase() !== "LIVE";
    $("accountTitle").textContent = paper ? "ACCOUNT · PAPER" : "ACCOUNT";
    if (a !== undefined) {
      $("accountTag").textContent = !a ? "Unavailable" : `${fHm.format(new Date())} IST`;
      money("acctBalance", a ? millis(a.balance) : null, false);
      money("acctStart", a ? millis(a.start) : null, false);
      money("acctDay", a ? millis(a.day_realised) : null, true);
      money("acctNight", a ? millis(a.night_realised) : null, true);
    }
    if (broker !== undefined) {
      const cash = millis(broker?.cash), used = millis(broker?.margin_used);
      $("acctBroker").textContent = cash === null ? "Unavailable" : `₹${nf0.format(cash)} / ₹${nf0.format(used ?? 0)}`;
    }
  }

  // ── Settings ─────────────────────────────────────────────────────────────
  // Settings: every instrument's pivot threshold, and which one the automatic trader enters.
  async function openSettings() {
    const dialog = $("settings"), rows = $("settingsRows"), status = $("settingsStatus");
    status.textContent = ""; status.removeAttribute("data-tone");
    let state, health = null;
    try { state = await fetchJson("/manual/state", 4000); } catch (_e) { toast("Settings are unavailable"); return; }
    try { health = await fetchJson("/manual/health", 4000); } catch (_e) { health = null; }
    const auto = state?.auto;
    const names = Object.keys(auto?.fixed || {});
    if (!names.length) { toast("Settings are unavailable"); return; }
    const checks = {};
    for (const r of health?.rows || []) checks[r.index] = r;
    // The check is for the settings it ran on; any other setting changed since makes it stale.
    const canon = (v) => JSON.stringify(v, (_k, x) => (x && typeof x === "object" && !Array.isArray(x))
      ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, x[k]])) : x);
    const stale = Boolean(health?.params && state?.auto?.params) && (() => {
      const a = { ...health.params }, b = { ...state.auto.params };
      delete a.index_pivots; delete b.index_pivots;          // thresholds are compared per row below
      return canon(a) !== canon(b);
    })();
    rows.replaceChildren(...names.map((name) => {
      const row = document.createElement("tr");
      const label = document.createElement("th"); label.scope = "row"; label.textContent = name;
      const fixedCell = document.createElement("td"); fixedCell.dataset.label = "Threshold (×cost)";
      const fixed = document.createElement("input");
      Object.assign(fixed, { type: "number", min: "0.5", max: "100", step: "0.5", required: true, value: auto.fixed[name] ? String(auto.fixed[name]) : "" });
      fixed.inputMode = "decimal"; fixed.dataset.fixed = name;
      fixed.setAttribute("aria-label", `${name} threshold, a multiple of cost`);
      fixedCell.append(fixed);
      const now = document.createElement("td"); now.className = "settings-now tnum";
      const pts = auto.threshold_pts?.[name];
      now.textContent = pts ? `${pts.toFixed(1)} pts` : "—";
      const retune = document.createElement("td"); retune.className = "settings-retune";
      const verdict = document.createElement("b"), why = document.createElement("small");
      retune.append(verdict, why);
      const showCheck = () => {
        const r = checks[name], value = Number(fixed.value);
        let v, text, title = "";
        if (!r) { v = "none"; text = "NOT CHECKED"; title = health ? "No check for this instrument yet." : "The retune check is unavailable."; }
        else if (value !== r.threshold) { v = "pending"; text = "CHECKED AFTER SAVE"; title = `The check covers ${r.threshold}, not ${fixed.value || "—"}.`; }
        else if (stale) { v = "pending"; text = "STALE"; title = "Settings other than the threshold changed after this check; it re-runs within a minute or two."; }
        else {
          v = { SWITCH: "switch", ALARM: "alarm", OK: "ok" }[r.verdict] || "none";
          text = r.verdict === "SWITCH" ? `SWITCH TO ${r.switch_to}` : r.verdict;
          title = [r.note, r.sessions ? `${r.sessions} counted session(s), ${r.first} to ${r.last}.` : "",
            ...(r.why_not || []).map((w) => `Not chosen: ${w}`)].filter(Boolean).join("\n");
        }
        retune.dataset.verdict = v; verdict.textContent = text;
        why.textContent = v === "pending" || !r ? "" : r.note || "";
        retune.title = title;
      };
      showCheck();
      fixed.addEventListener("input", showCheck);
      const trade = document.createElement("td"); trade.className = "settings-trade";
      const pickLabel = document.createElement("label");
      const pickText = document.createElement("span"); pickText.textContent = "Trade";
      const pick = document.createElement("input");
      Object.assign(pick, { type: "radio", name: "trade", value: name, checked: name === auto.trade });
      pick.setAttribute("aria-label", `Trade ${name}`);
      pickLabel.append(pickText, pick);
      trade.append(pickLabel);
      row.append(label, fixedCell, now, retune, trade);
      return row;
    }));
    const when = $("settingsChecked");
    when.textContent = health?.at
      ? `Retune check ${health.at.slice(0, 16).replace("T", " ")} IST, on finished sessions only. Hover or long-press a verdict for its evidence.`
      : "Retune check not run yet: it runs after each finished session and each threshold change.";
    dialog.showModal();
  }

  async function saveSettings(event) {
    event.preventDefault();
    const status = $("settingsStatus"), save = $("settingsSave");
    const say = (tone, text) => { status.dataset.tone = tone; status.textContent = text; };
    const fixed = {};
    for (const box of $("settingsRows").querySelectorAll("input[data-fixed]")) {
      const value = Number(box.value);
      if (!box.value || !Number.isFinite(value) || value < 0.5 || value > 100) {
        box.focus(); say("bad", `${box.dataset.fixed}: enter 0.5 to 100`); return;
      }
      fixed[box.dataset.fixed] = value;
    }
    const trade = $("settingsForm").querySelector("input[name=trade]:checked")?.value;
    if (!trade) { say("bad", "Choose an instrument to trade"); return; }
    save.disabled = true;
    say("work", "Saving…");
    try {
      const res = await fetch("/manual/settings", {
        method: "POST", cache: "no-store",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ fixed, trade }),
      });
      const data = await res.json().catch(() => ({}));
      if (res.status === 202) { $("settings").close(); toast(`Saved. Trading ${trade}; each index switches when flat.`); }
      else say("bad", data?.error || `Refused (${res.status})`);
    } catch (_e) {
      say("bad", "The desk did not answer. The settings may not have changed.");
    }
    save.disabled = false;
  }

  // ── Option legs ──────────────────────────────────────────────────────────
  // Every index's legs as its builder last published them, and the held
  // position's as the ledger recorded them. The list is rebuilt only when the
  // payload changes, so the poll does not churn it under the pointer.
  const LEG_LABEL = { blocked: "Blocked", idle: "No legs requested", offline: "Offline" };
  let legsSeen = "";

  function node(tag, className, text) {
    const n = document.createElement(tag);
    if (className) n.className = className;
    if (text !== undefined) n.textContent = text;
    return n;
  }

  function legRow(row) {
    const li = node("li");
    li.dataset.state = row.state;
    const name = node("span", "leg-index");
    name.append(node("b", "", row.index),
      node("small", "", [row.exchange, row.lot, row.state === "held" ? "HELD" : null].filter(Boolean).join(" · ")));
    const pair = node("span", "leg-pair");
    if (row.state === "armed" || row.state === "held") {
      for (const [cls, mark, symbol] of [["leg-sell", "SELL", row.sell], ["leg-buy", "BUY", row.buy]]) {
        const line = node("span", cls, symbol || "—");
        line.prepend(node("i", "", mark));
        pair.append(line);
      }
    } else {
      // Refusals name their cause; idle is the normal state outside
      // the entry window and says so without the builder's full sentence.
      const label = LEG_LABEL[row.state] || "Unknown";
      const showNote = row.note && row.state === "blocked";
      const note = node("span", "leg-note", showNote ? `${label} · ${row.note}` : label);
      if (row.note) note.title = row.note;
      pair.append(note);
    }
    li.append(name, pair);
    return li;
  }

  async function refreshLegs() {
    let data;
    try {
      data = await fetchJson("/option_legs", 6000);
      if (!Array.isArray(data?.legs)) throw new Error("unavailable");
    } catch (_e) {
      // The last list stays up, marked: blanking six rows for one dropped poll
      // reads as six builders failing at once.
      legsSeen = "";
      el.legsTag.dataset.legs = "error";
      el.legsTag.textContent = "Unavailable";
      el.legsNote.textContent = el.legs.childElementCount
        ? "Leg feed unavailable · showing the last reading" : "Leg feed unavailable";
      return;
    }

    const seen = JSON.stringify([data.legs, data.held, data.ledger]);
    if (seen !== legsSeen) {
      legsSeen = seen;
      el.legs.replaceChildren(...data.legs.map(legRow));
    }

    const armed = data.legs.filter((row) => row.state === "armed").length;
    const held = data.held?.index || null;
    el.legsTag.dataset.legs = held ? "held" : armed ? "armed" : "idle";
    el.legsTag.textContent = held ? `Held · ${held}` : armed ? `${armed}/${data.legs.length} armed` : "Standing by";

    const at = parseFeedTime(data.as_of_ist);
    el.legsNote.textContent = data.ledger === false
      // A ledger that did not answer is not a flat book; say so rather than let
      // the built legs read as "nothing held".
      ? "Position ledger unavailable · a held position is not shown"
      : `Sell = short put · Buy = hedge · lot in units${Number.isFinite(at) ? ` · ${fClock.format(new Date(at))}` : ""}`;
  }

  async function refreshResources() {
    try {
      const data = await fetchJson("/resource_usage", 6000);
      const bot = data?.bot || {}, db = data?.database || {};
      const botCpu = num(bot.cpu_percent), dbCpu = num(db.cpu_percent);
      const botMem = num(bot.memory_mb);
      const dbUp = db.available !== false;

      el.botCpu.textContent = botCpu === null ? "—" : `${botCpu.toFixed(1)}%`;
      el.dbCpu.textContent = dbCpu === null ? "—" : `${dbCpu.toFixed(1)}%`;
      health.bot = cpuHealth(botCpu);
      health.db = cpuHealth(dbCpu, dbUp);
      el.botLed.dataset.health = health.bot;
      el.dbLed.dataset.health = health.db;

      el.hostMeta.textContent = [
        botMem === null ? null : `${botMem.toFixed(0)} MB`,
        Number.isFinite(Number(bot.threads)) ? `${bot.threads} threads` : null,
        uptime(num(bot.uptime_sec)),
        dbUp ? (db.size_pretty || null) : "database metrics unavailable",
      ].filter(Boolean).join(" · ") || "Responding";

      healthNote = null;
      updateHealth();
    } catch (_e) {
      health.bot = "error"; health.db = "error";
      healthNote = "Unavailable";
      updateHealth();
      el.botLed.dataset.health = "error";
      el.dbLed.dataset.health = "error";
      el.botCpu.textContent = "—"; el.dbCpu.textContent = "—";
      el.hostMeta.textContent = "Host metrics unavailable";
    }
  }

  // ── WebSocket ────────────────────────────────────────────────────────────
  // The socket, and where each index's price is coming from right now. A REST
  // stand-in keeps entries alive through a dropped feed but is slower than a
  // tick, so it is worth seeing; an index priced by neither cannot trade.
  const SOURCE = { ws: "WebSocket", rest: "REST fallback", stale: "No fresh price", none: "Not priced yet" };
  const shortName = (name) => (name === "NIFTYNXT50" ? "NX" : String(name || "?")[0]);
  let lastTicks = null;

  async function refreshFeed() {
    let data = null;
    try { data = await fetchJson("/feed_health", 6000); } catch (_e) { /* reported below */ }
    const feeds = Array.isArray(data?.feeds) ? data.feeds : [];
    if (!data?.available) {
      lastTicks = null;
      health.ws = "unknown";
      el.wsLed.dataset.health = "unknown";
      el.wsRate.textContent = "—";
      el.wsMeta.textContent = "Feed monitor unavailable";
      el.feeds.replaceChildren();
      return updateHealth();
    }

    const count = (src) => feeds.filter((f) => f.source === src).length;
    const dead = count("stale") + count("none"), rest = count("rest");
    // Out of session the indices stop printing, so a silent feed is the market
    // closed rather than a fault, and only the socket itself is judged.
    health.ws = !data.connected ? "error"
      : !data.in_session ? "healthy"
      : dead ? "error" : rest ? "warning" : "healthy";
    el.wsLed.dataset.health = health.ws;

    const ticks = num(data.ticks), at = Date.now();
    const rate = lastTicks && ticks !== null && ticks >= lastTicks.ticks
      ? (ticks - lastTicks.ticks) / Math.max(0.5, (at - lastTicks.at) / 1000) : null;
    lastTicks = ticks === null ? null : { ticks, at };
    el.wsRate.textContent = !data.connected ? (data.reconnecting ? "Reconnecting" : "Down")
      : rate === null ? "Live" : `${rate < 10 ? rate.toFixed(1) : Math.round(rate)}/s`;

    const reconnects = Number(data.reconnects) || 0;
    el.wsMeta.textContent = [
      data.connected ? uptime(num(data.uptime_s)) : data.failures ? `${data.failures} failed attempts` : "disconnected",
      `${reconnects} reconnect${reconnects === 1 ? "" : "s"}`,
      !data.in_session ? "market closed"
        : rest || dead ? `${feeds.length - rest - dead}/${feeds.length} on ticks` : "all on ticks",
    ].filter(Boolean).join(" · ");

    el.feeds.replaceChildren(...feeds.map((f) => {
      const cell = document.createElement("span");
      cell.setAttribute("role", "listitem");
      cell.dataset.source = data.in_session || f.source === "ws" ? f.source : "none";
      cell.textContent = shortName(f.index);
      const age = num(f.tick_age_s);
      cell.title = `${f.index} · ${SOURCE[f.source] || f.source}${age === null ? "" : ` · last tick ${age.toFixed(1)} s ago`}`;
      return cell;
    }));
    updateHealth();
  }

  async function refreshEngine() {
    try { engines = (await fetchJson("/entry_gates", 6000)) || {}; }
    catch (_e) {
      // Keep the last reading on screen rather than blanking the bay for one
      // dropped poll. The feed pod is already the place that reports a link
      // which is actually down.
      return;
    }
    renderEngine();
  }

  async function refreshPing() {
    try { applyPing(await fetchJson("/broker_ping", 9000)); }
    catch (_e) { applyPing(null); }
  }

  // ── Charted index ────────────────────────────────────────────────────────
  // Switching index is switching instruments, not filtering a view: every
  // reading on the page belongs to the old one until its feed answers. So the
  // session baselines, the last tick and the plot are dropped on the spot and
  // the veil goes back up, rather than leaving NIFTY's price above SENSEX's
  // bars for a poll.
  function selectIndex(next) {
    if (!INDICES.includes(next) || next === index) return;
    index = next;
    el.quoteIndex.textContent = next;
    fitIndexName();
    try { window.localStorage.setItem("lune.index", next); } catch (_e) { /* private mode */ }

    for (const chip of el.picker.querySelectorAll(".picker-chip"))
      chip.setAttribute("aria-pressed", String(chip.dataset.index === next));

    candles = [];
    sessionOpen = null;
    hasTick = false; lastTick = null;
    chart.setData([], { hh: [], ll: [] });
    setPrice(null);
    el.delta.textContent = `Loading ${next}`; tone(el.delta, 0);
    el.asOf.textContent = "—";
    el.barCount.textContent = "0 bars";
    setVeil("waiting", `Loading ${next}`, `Fetching the ${timeframe} feed for ${next}.`);
    document.title = `Lune · ${next} console`;

    // The bay follows the chart, so the two agree unless the operator has
    // deliberately pointed the bay somewhere else.
    selectEngineIndex(next);
    refreshSnapshot(); openPriceStream();
    announceIndex();
  }

  // Switching the bay costs no fetch: `/entry_gates` already carries every
  // index, so the other two are on screen the moment the chip is pressed.
  function selectEngineIndex(next) {
    if (!INDICES.includes(next)) return;
    engineIndex = next;
    if (el.enginePicker) {
      for (const chip of el.enginePicker.querySelectorAll(".picker-chip"))
        chip.setAttribute("aria-pressed", String(chip.dataset.index === next));
    }
    renderEngine();
  }

  if (el.enginePicker) {
    el.enginePicker.addEventListener("click", (event) => {
      const chip = event.target.closest(".picker-chip");
      if (chip) selectEngineIndex(chip.dataset.index);
    });
    // The chips are stamped with the server's default; a remembered chart index
    // may not be it.
    for (const chip of el.enginePicker.querySelectorAll(".picker-chip"))
      chip.setAttribute("aria-pressed", String(chip.dataset.index === engineIndex));
  }

  if (el.picker) {
    el.picker.addEventListener("click", (event) => {
      const chip = event.target.closest(".picker-chip");
      if (chip) selectIndex(chip.dataset.index);
    });
    for (const chip of el.picker.querySelectorAll(".picker-chip"))
      chip.setAttribute("aria-pressed", String(chip.dataset.index === index));
  }

  // History refreshes on demand; it does not add another database poller.
  const filters = $("pnlFilters");
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Kolkata", year: "numeric", month: "2-digit", day: "2-digit" });
  const istDay = () => today.format(new Date());
  $("pnlEnd").value = istDay();
  $("pnlStart").value = `${istDay().slice(0, 8)}01`;
  let historyRequest = 0;
  async function refreshHistory() {
    $("pnlStart").setCustomValidity($("pnlStart").value > $("pnlEnd").value ? "Start date must not follow end date." : "");
    if (!filters.reportValidity()) return;
    const requestId = ++historyRequest;
    $("pnlStatus").textContent = "Loading trade history…";
    $("pnlResults").hidden = true;
    $("pnlApply").disabled = true;
    try {
      const data = await fetchJson(`/pnl_history?${new URLSearchParams(new FormData(filters))}`, 10000);
      if (requestId !== historyRequest) return;
      const selected = $("pnlInstrument").value;
      const names = [...new Set([...data.available_instruments, ...(selected ? [selected] : [])])];
      $("pnlInstrument").replaceChildren(new Option("All instruments", ""), ...names.map(name => new Option(name, name)));
      $("pnlInstrument").value = selected;
      $("historyTotal").textContent = rupees(data.pnl);
      tone($("historyTotal"), data.pnl);
      $("historyMeta").textContent = `₹ · AFTER ${rupees(-data.charges).replace("−", "")} CHARGES`;
      $("historyTrades").textContent = data.trades;
      $("historyWinRate").textContent = data.trades ? `${Math.round(data.wins / data.trades * 100)}%` : "—";
      const cumulative = [0];
      for (const day of data.daily) cumulative.push(cumulative[cumulative.length - 1] + day.pnl);
      const bottom = Math.min(...cumulative), top = Math.max(...cumulative);
      $("historyLine").setAttribute("d", cumulative.map((value, i) =>
        `${i ? "L" : "M"}${10 + i * 200 / Math.max(1, cumulative.length - 1)},${top === bottom ? 20 : 35 - (value - bottom) * 30 / (top - bottom)}`
      ).join(" "));
      tone($("historyCurve"), data.pnl);
      const bookRows = (data.books || []).map((row) => ({ ...row, label: BOOK_NAME[row.book] || row.book }));
      for (const [id, rows, key] of [["dailyPnl", [...data.daily].reverse(), "date"], ["instrumentPnl", data.instruments, "instrument"], ["bookPnl", bookRows, "label"]]) {
        const fragment = document.createDocumentFragment();
        const largest = Math.max(1, ...rows.map(row => Math.abs(row.pnl)));
        for (const row of rows) {
          const tr = document.createElement("tr");
          for (const value of [row[key], row.trades, rupees(row.pnl)]) {
            const td = document.createElement("td"); td.textContent = value; tr.appendChild(td);
          }
          tone(tr.lastElementChild, row.pnl);
          tr.lastElementChild.style.setProperty("--magnitude", `${Math.abs(row.pnl) / largest * 100}%`);
          fragment.appendChild(tr);
        }
        $(id).replaceChildren(fragment);
      }
      $("pnlResults").hidden = !data.trades;
      $("pnlStatus").textContent = data.trades
        ? `${data.start} → ${data.end} · ${data.mode === "ALL" ? "All execution modes" : data.mode} · ${{ DAY: "Day book", NIGHT: "Night book" }[data.book] || "Day + night"} · ${selected || "All instruments"}`
        : "No closed trades for this selection. Try another period or instrument.";
    } catch (_error) {
      if (requestId === historyRequest) $("pnlStatus").textContent = "Trade history unavailable. Apply filters to retry.";
    } finally {
      if (requestId === historyRequest) $("pnlApply").disabled = false;
    }
  }
  filters.addEventListener("submit", event => { event.preventDefault(); refreshHistory(); });
  filters.addEventListener("input", () => $("pnlStart").setCustomValidity(""));
  document.querySelector(".periods").addEventListener("click", event => {
    const days = event.target.closest("[data-days]")?.dataset.days;
    if (!days) return;
    const end = istDay(), start = new Date(`${end}T00:00:00Z`);
    if (days === "month") start.setUTCDate(1);
    else start.setUTCDate(start.getUTCDate() - Number(days) + 1);
    $("pnlStart").value = start.toISOString().slice(0, 10);
    $("pnlEnd").value = end;
    refreshHistory();
  });
  $("pnlMode").value = ["LIVE", "TEST"].includes(body.dataset.mode) ? body.dataset.mode : "ALL";
  refreshHistory();

  // ── Wiring: books, settings ──────────────────────────────────────
  $("bookFilter").addEventListener("click", (event) => {
    const button = event.target.closest("[data-book]");
    if (button) selectBookFilter(button.dataset.book);
  });
  $("settingsOpen").addEventListener("click", openSettings);
  $("settingsForm").addEventListener("submit", saveSettings);
  for (const id of ["settingsClose", "settingsCancel"]) $(id).addEventListener("click", () => $("settings").close());
  selectBookFilter(bookFilter);

  // ── Scheduling ───────────────────────────────────────────────────────────
  function every(task, interval) {
    const run = async () => {
      if (!document.hidden) await task();
      window.setTimeout(run, interval);
    };
    run();
  }

  document.addEventListener("visibilitychange", () => {
    // A hidden tab holds no stream open, as it runs no pollers.
    if (document.hidden) { priceStream?.close(); priceStream = null; return; }
    openPriceStream();
    refreshSnapshot(); refreshBooks(); refreshPing(); refreshEngine(); refreshLegs(); refreshFeed();
  });

  window.addEventListener("resize", () => { chart.measure(); chart.schedule(); fitIndexName(); });
  // Widths measured in the fallback face are wrong once DM Sans arrives.
  fitIndexName();
  document.fonts?.ready.then(fitIndexName);

  chart.measure();
  announceIndex();
  document.title = `Lune · ${index || "NIFTY"} console`;
  tickClock();
  window.setInterval(tickClock, 1000);
  every(refreshSnapshot, snapshotPollMs);
  if (!document.hidden) openPriceStream();
  // The session bay, today's trades and the account, from one read.
  every(refreshBooks, 2000);
  // Shoonya's funds: a broker call, so twice a minute is plenty.
  every(refreshBroker, 30000);
  // The gates are scored against the tick now, not against the last close, so
  // the bay keeps the price's own cadence.
  every(refreshEngine, 1000);
  every(refreshResources, 6000);
  // The builders republish every second; half that is fresh enough to read
  // and keeps six rows of DOM from being rebuilt on every tick.
  every(refreshLegs, 2000);
  every(refreshFeed, 2000);
  // Matches the probe's own TTL: polling faster only re-serves a cached sample,
  // and polling slower lets the ladder go stale between candles.
  every(refreshPing, 5000);
})();
