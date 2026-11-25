/* Lune · HH/LL tick chart — the day book's own chart, inside the console.
   The market chart switches between V3 (v3's 5-minute candles, Lune-v6.js) and
   HH/LL: every websocket tick of the session, with the HH/LL engine's pivots the
   day desk trades on. `DeskChart` is kept as the backtester copies it (bt.js),
   candle drawing included; here it is only ever given ticks. Vanilla, no dependencies; the canvas
   palette is read out of Lune-v5.css, as the algo chart's is. */
(() => {
  "use strict";

  const body = document.body;
  const $ = (id) => document.getElementById(id);
  if (!$("tickView")) return;

  const store = {
    get(key) { try { return window.localStorage.getItem(key); } catch (_e) { return null; } },
    set(key, value) { try { window.localStorage.setItem(key, value); } catch (_e) { /* private mode */ } },
  };

  // ── Formatting ───────────────────────────────────────────────────────────
  const nf = new Intl.NumberFormat("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const nf0 = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 });
  const IST = { timeZone: "Asia/Kolkata", hourCycle: "h23" };
  const fClock = new Intl.DateTimeFormat("en-GB", { ...IST, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const fHm = new Intl.DateTimeFormat("en-GB", { ...IST, hour: "2-digit", minute: "2-digit" });
  const num = (x) => (x === null || x === undefined || x === "" ? null : (Number.isFinite(Number(x)) ? Number(x) : null));
  const fmt = (x) => (Number.isFinite(x) ? nf.format(x) : "—");

  // Bar times arrive as IST wall clock ("+05:30"); the digits are the truth.
  // Same rule as Lune-v6.js, so the two charts cannot plot a bar apart.
  const IST_MS = 5.5 * 3600 * 1000;
  function parseFeedTime(x) {
    const m = String(x ?? "").match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?/);
    return m ? Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +(m[6] || 0)) - IST_MS : Number.NaN;
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


  const root = getComputedStyle(document.documentElement);
  const v = (n, f) => (root.getPropertyValue(n) || f).trim();
  const P = {
    up: v("--up", "#3ed6ad"), upLine: v("--up-line", "rgba(62,214,173,.38)"), upSoft: v("--up-soft", "rgba(62,214,173,.13)"),
    down: v("--down", "#ff607c"), downLine: v("--down-line", "rgba(255,96,124,.38)"),
    accent: v("--accent", "#ffb45c"), axis: v("--axis", "#71747b"), grid: v("--grid", "rgba(255,255,255,.055)"),
    gridSoft: v("--grid-strong", "rgba(255,255,255,.025)"), cross: v("--cross", "rgba(255,255,255,.28)"),
    crossDot: v("--cross-dot", "#fff"), badgeInk: v("--ink-badge", "#070707"), mono: v("--mono", "ui-monospace, monospace"),
  };

  // ── Chart ────────────────────────────────────────────────────────────────
  // Two shapes on one canvas. TICK is every tick of the session on a time
  // axis - nothing sampled away, nothing capped; past one tick per pixel each
  // column draws its full range, so a spike can never fall between pixels.
  // 1M/5M/15M are candles, one slot per bar like the algo chart's.
  const AXIS_ROW = 18;
  const MIN_BARS = 10;          // candles zoomed all the way in
  const MIN_SPAN = 10e3;        // TICK zoomed all the way in, in ms
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
  const lowerBound = (xs, x) => { let lo = 0, hi = xs.length; while (lo < hi) { const mid = (lo + hi) >> 1; if (xs[mid] < x) lo = mid + 1; else hi = mid; } return lo; };

  // The view, panned and zoomed by drag, wheel, pinch and keys:
  //   candles  `span` bars wide (null: every bar) ending `offset` bars before the newest
  //   TICK     `tSpan` ms wide (null: the whole session) ending at `tTo` (epoch ms; null: the newest tick),
  //            opening on the last `window` ms (null: the whole session)
  //   both     the price range scaled by `yZoom` (dragging the price scale)
  // An offset of 0 / a null tTo follows the market; double-click or End returns there.
  class DeskChart {
    constructor(canvas) {
      this.canvas = canvas;
      this.ctx = canvas.getContext("2d", { alpha: true });
      this.kind = "tick"; this.ticks = { t: [], p: [] }; this.bars = []; this.entry = null;
      this.hover = null; this.hoverY = null; this.geom = null; this.frame = null; this.onSelect = null;
      this.pointers = new Map(); this.drag = null; this.pinch = null;
      this.marks = []; this.barMs = 0;   // HH/LL pivots and this desk's IN/OUT trades, by timestamp
      this.window = 5 * 60e3;
      this.resetView();
      new ResizeObserver(() => this.schedule()).observe(canvas);
      canvas.addEventListener("pointermove", (e) => { if (!this.onDrag(e)) this.onPointer(e); });
      canvas.addEventListener("pointerleave", () => { if (!this.drag) this.clearHover(); });
      canvas.addEventListener("keydown", (e) => this.onKey(e));
      canvas.addEventListener("wheel", (e) => this.onWheel(e), { passive: false });
      canvas.addEventListener("pointerdown", (e) => this.onDown(e));
      for (const type of ["pointerup", "pointercancel"]) canvas.addEventListener(type, (e) => this.onUp(e));
      canvas.addEventListener("dblclick", (e) => this.onDouble(e));
    }

    get size() { return this.kind === "tick" ? this.ticks.t.length : this.bars.length; }

    resetView() { this.span = null; this.offset = 0; this.tSpan = this.window; this.tTo = null; this.yZoom = 1; }

    // Another frame, index or session opens on the newest data; an update
    // keeps a view scrolled back on what it showed as new data arrives.
    setMarks(marks, barMs) { this.marks = marks; this.barMs = barMs; this.schedule(); }

    set(kind, data, entry) {
      const changed = kind !== this.kind;
      const firstOf = (k, d) => (k === "tick" ? d.t[0] : d[0]?.timestamp);
      const before = firstOf(this.kind, this.kind === "tick" ? this.ticks : this.bars), was = this.bars.length;
      this.kind = kind;
      if (kind === "tick") this.ticks = data; else this.bars = data;
      // a new series voids the drawn geometry until the next frame draws it
      if (changed || before === undefined || before !== firstOf(kind, data)) { this.resetView(); this.geom = null; }
      else if (kind !== "tick" && this.offset > 0) this.offset += this.bars.length - was;
      this.entry = Number.isFinite(entry) ? entry : null;
      if (changed || (this.hover !== null && this.hover >= this.size)) { this.hover = null; this.hoverY = null; }
      this.schedule(); this.emit();
    }

    local(e) {
      const b = this.canvas.getBoundingClientRect();
      return { x: e.clientX - b.left, y: e.clientY - b.top };
    }

    // Candles move by whole and part bars, never past the oldest or the newest.
    setOffset(offset) {
      const slots = this.geom && this.geom.slots ? this.geom.slots : this.bars.length;
      this.offset = clamp(offset, 0, Math.max(0, this.bars.length - slots));
      if (this.offset < 1e-6) this.offset = 0;
    }

    // TICK: a view ending before the newest tick stays there as ticks stream;
    // reaching it, the view follows again.
    setTo(to) {
      const g = this.geom;
      this.tTo = to >= g.tEnd ? null : Math.max(to, g.tStart + g.t1 - g.t0);
    }

    // Dragged `dx` px: right shows older, left newer.
    panBy(dx) {
      const g = this.geom;
      if (!g) return;
      if (this.kind === "tick") { if (this.tSpan !== null) this.setTo(g.t1 - (dx / g.width) * (g.t1 - g.t0)); }
      else this.setOffset(this.offset + dx / g.stride);
      this.schedule();
    }

    // Zoomed by `factor` (above 1: out) about the point at `x`, which keeps its
    // place; a view following the market zooms about its right edge and keeps following.
    zoomAt(factor, x) {
      const g = this.geom;
      if (!g) return;
      const following = this.kind === "tick" ? this.tTo === null : this.offset === 0;
      const r = following ? 1 : clamp((x - g.left) / g.width, 0, 1);
      if (this.kind === "tick") {
        const was = g.t1 - g.t0, full = g.tEnd - g.tStart;
        const at = g.t1 - was * (1 - r);
        const span = clamp(was * factor, Math.min(MIN_SPAN, full), full);
        this.tSpan = span >= full ? null : span;
        if (this.tSpan === null) this.tTo = null;
        else if (!following) this.tTo = clamp(at + span * (1 - r), g.tStart + span, g.tEnd);
        if (this.tTo !== null && this.tTo >= g.tEnd) this.tTo = null;
      } else {
        const n = this.bars.length, was = g.slots;
        let span = Math.round(was * factor);
        if (span === was) span += factor > 1 ? 1 : -1;
        span = clamp(span, Math.min(MIN_BARS, n), n);
        // bars right of the point: (1 - r) of the view before, (1 - r) of it after
        const right = this.offset + was * (1 - r);
        this.span = span >= n ? null : span;
        this.offset = clamp(right - span * (1 - r), 0, n - span);
        if (this.offset < 1e-6) this.offset = 0;
      }
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
      if (d.gutter) {               // the price scale: drag down to squeeze the line, up to stretch
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

    // Double-click: the price scale goes back to fitting the data; anywhere else the whole view resets.
    onDouble(e) {
      if (!this.geom) return;
      if (this.local(e).x > this.geom.left + this.geom.width) this.yZoom = 1;
      else this.resetView();
      this.schedule();
    }

    emit() {
      if (!this.onSelect) return;
      const i = this.hover === null ? this.size - 1 : this.hover;
      this.onSelect(i >= 0 ? i : null, this.hover !== null);
    }

    schedule() {
      if (this.frame !== null) return;
      this.frame = requestAnimationFrame(() => { this.frame = null; this.render(); });
    }

    clearHover() { this.hover = null; this.hoverY = null; this.emit(); this.schedule(); }

    onPointer(e) {
      const g = this.geom;
      if (!g || this.pinch) return;
      const { x, y } = this.local(e);
      if (!this.drag) this.canvas.style.cursor = x > g.left + g.width ? "ns-resize" : "";
      if (x < g.left || x > g.left + g.width || y < g.top || y > g.top + g.height) return this.clearHover();
      // `hover` indexes the whole series; the view holds `start`..`end`.
      this.hover = this.kind === "tick"
        ? clamp(lowerBound(this.ticks.t, g.t0 + ((x - g.left) / g.width) * (g.t1 - g.t0)), g.start, g.end - 1)
        : clamp(g.start + Math.floor(g.end - g.start - (g.edge - x) / g.stride), g.start, g.end - 1);
      this.hover = clamp(this.hover, 0, this.size - 1);
      this.hoverY = y;
      this.emit(); this.schedule();
    }

    // The arrows walk the readout across the data, panning at either edge;
    // + and - zoom, End returns to the newest.
    onKey(e) {
      const n = this.size, g = this.geom;
      if (!n || !g) return;
      if (["+", "=", "-", "_"].includes(e.key)) {
        e.preventDefault();
        return this.zoomAt(e.key === "+" || e.key === "=" ? 0.8 : 1.25, g.left + g.width / 2);
      }
      if (e.key === "End") { e.preventDefault(); this.resetView(); return this.schedule(); }
      if (!["ArrowLeft", "ArrowRight", "Escape"].includes(e.key)) return;
      e.preventDefault();
      if (e.key === "Escape") return this.clearHover();
      const from = this.hover === null ? g.end - 1 : this.hover;
      this.hover = Math.max(0, Math.min(n - 1, from + (e.key === "ArrowRight" ? 1 : -1)));
      if (this.kind === "tick") {
        const at = this.ticks.t[this.hover];
        if (at > g.t1) this.setTo(at);
        else if (at < g.t0) this.setTo(at + (g.t1 - g.t0));
      } else if (this.hover < n - g.slots - this.offset) this.setOffset(n - g.slots - this.hover);
      else if (this.hover > n - 1 - this.offset) this.setOffset(n - 1 - this.hover);
      this.hoverY = null;
      this.emit(); this.schedule();
    }

    round(x, y, w, h, r) {
      const c = this.ctx, rr = Math.min(r, w / 2, h / 2);
      c.beginPath();
      c.moveTo(x + rr, y);
      c.arcTo(x + w, y, x + w, y + h, rr); c.arcTo(x + w, y + h, x, y + h, rr);
      c.arcTo(x, y + h, x, y, rr); c.arcTo(x, y, x + w, y, rr);
      c.closePath();
    }

    badge(text, y, fill, W, right, ink) {
      const c = this.ctx, bw = right - 14, bh = 20;
      this.round(W - right + 8, y - bh / 2, bw, bh, 6);
      c.fillStyle = fill; c.fill();
      c.fillStyle = ink; c.textAlign = "center"; c.textBaseline = "middle";
      c.fillText(text, W - right + 8 + bw / 2, y + 0.5);
    }

    render() {
      const c = this.ctx, W = this.canvas.clientWidth, H = this.canvas.clientHeight;
      if (!c || W < 1 || H < 1) return;
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const tw = Math.round(W * dpr), th = Math.round(H * dpr);
      if (this.canvas.width !== tw || this.canvas.height !== th) { this.canvas.width = tw; this.canvas.height = th; }
      c.setTransform(dpr, 0, 0, dpr, 0, 0);
      c.clearRect(0, 0, W, H);
      if (!this.size) { this.geom = null; return; }

      const narrow = W < 620;
      const top = 20, right = narrow ? 66 : 78, left = 14;
      const width = Math.max(1, W - left - right), height = Math.max(1, H - top - 26 - AXIS_ROW);
      const tick = this.kind === "tick";
      const { t, p } = this.ticks, all = this.bars, n = this.size;

      // What is in view. TICK: the ticks inside the window, and one either
      // side so the line runs to both edges. Candles: whole bars from the right
      // edge, plus the one cut by the left edge while the view sits between bars.
      let start = 0, end = n, from = 0, to = n, slots = 0, stride = 0, edge = 0, t0 = 0, t1 = 0, tStart = 0, tEnd = 0;
      if (tick) {
        tStart = t[0]; tEnd = Math.max(t[n - 1], t[0] + 1000);
        const tSpan = this.tSpan === null ? tEnd - tStart : Math.min(this.tSpan, tEnd - tStart);
        t1 = this.tTo === null ? tEnd : clamp(this.tTo, tStart + tSpan, tEnd); t0 = t1 - tSpan;
        from = lowerBound(t, t0); to = from;
        while (to < n && t[to] <= t1) to += 1;
        start = Math.max(0, from - 1); end = Math.min(n, to + 1);
        if (to <= from) { from = start; to = end; }    // no tick inside: the price held across it
      } else {
        slots = clamp(this.span ?? n, 1, n);
        this.offset = clamp(this.offset, 0, n - slots);
        const whole = Math.floor(this.offset), part = this.offset - whole;
        end = n - whole; start = Math.max(0, end - slots - (part > 0 ? 1 : 0));
        from = start; to = end;
        stride = width / slots; edge = left + width + part * stride;
      }

      let min = Infinity, max = -Infinity;
      if (tick) for (let i = from; i < to; i += 1) { if (p[i] < min) min = p[i]; if (p[i] > max) max = p[i]; }
      else for (let i = start; i < end; i += 1) { const k = all[i]; if (k.low < min) min = k.low; if (k.high > max) max = k.high; }
      const inView = (ts) => (tick ? ts >= t0 && ts <= t1 : ts >= all[start].timestamp && ts < all[end - 1].timestamp + this.barMs);
      const marks = this.marks.filter((m) => inView(m.timestamp));
      for (const m of marks) { if (m.price < min) min = m.price; if (m.price > max) max = m.price; }
      // An entry close to the data widens the scale so it shows; a far-away one
      // is not allowed to flatten the view into a line.
      const near = (max - min) || max * 0.001;
      const everything = this.yZoom === 1 && (tick ? this.tSpan === null : this.span === null);
      if (this.entry !== null && (everything || (this.entry >= min - near * 0.6 && this.entry <= max + near * 0.6))) {
        min = Math.min(min, this.entry); max = Math.max(max, this.entry);
      }
      const pad = Math.max((max - min) * 0.085, max * 0.0005, 1);
      min -= pad; max += pad;
      if (this.yZoom !== 1) {
        const mid = (min + max) / 2, half = (max - min) / 2 * this.yZoom;
        min = mid - half; max = mid + half;
      }
      const span = max - min || 1;
      const yAt = (v_) => top + ((max - v_) / span) * height;

      const xAt = tick ? (ts) => left + ((ts - t0) / (t1 - t0)) * width : (i) => edge - (end - i - 0.5) * stride;
      this.geom = { left, top, width, height, stride, slots, start, end, edge, t0, t1, tStart, tEnd };

      c.save();
      c.font = `${narrow ? 9.5 : 10.5}px ${P.mono}`;
      c.lineWidth = 1; c.textBaseline = "middle";
      // The last price is the newest tick or bar wherever the view is; panned or
      // scaled off the plot, its line and badge are not drawn.
      const last = tick ? p[n - 1] : all[n - 1].close;
      const lastShown = last >= min && last <= max;
      const lastY = lastShown ? yAt(last) : -1e9;
      const rows = height < 340 ? 4 : 6;
      for (let i = 0; i <= rows; i += 1) {
        const y = top + (i / rows) * height;
        c.beginPath(); c.strokeStyle = P.grid;
        c.moveTo(left, Math.round(y) + 0.5); c.lineTo(left + width, Math.round(y) + 0.5); c.stroke();
        if (Math.abs(y - lastY) < 13) continue;
        c.fillStyle = P.axis; c.textAlign = "right";
        c.fillText(fmt(max - (i / rows) * span), W - 12, y);
      }

      // Time axis. TICK: evenly spaced labels, each clamped whole inside the
      // plot. Candles: labelled by the bar's place in the whole series, so
      // labels move with the bars as they pan; one that would not print whole is skipped.
      const labels = width < 420 ? 3 : width < 820 ? 5 : 8;
      const grid = (x, label, at) => {
        c.beginPath(); c.strokeStyle = P.gridSoft;
        c.moveTo(Math.round(x) + 0.5, top); c.lineTo(Math.round(x) + 0.5, top + height); c.stroke();
        c.fillStyle = P.axis; c.textAlign = "center";
        c.fillText(label, at, top + height + 12);
      };
      // zoomed to minutes, the labels carry seconds so no two read the same
      const clock = (t1 - t0) / Math.max(1, labels - 1) < 60e3 ? fClock : fHm;
      if (tick) for (let i = 0; i < labels; i += 1) {
        const ts = t0 + (i / Math.max(1, labels - 1)) * (t1 - t0), x = xAt(ts);
        const label = clock.format(new Date(ts)), half = c.measureText(label).width / 2;
        if (x - half < left) continue;
        grid(x, label, Math.min(x, left + width - half));
      } else {
        const every = Math.max(1, Math.ceil(slots / labels));
        for (let i = start; i < end; i += 1) {
          if (i % every) continue;
          const x = xAt(i), label = fHm.format(new Date(all[i].timestamp)), half = c.measureText(label).width / 2;
          if (x - half < left || x + half > left + width) continue;
          grid(x, label, x);
        }
      }

      const rising = tick ? last >= p[0] : last >= all[n - 1].open;
      // The plot is clipped to its box: a bar half off either edge is cut there.
      c.save();
      c.beginPath(); c.rect(left, 0, width, H); c.clip();
      if (tick) this.drawTicks(xAt, yAt, top, height, start, end);
      else this.drawBars(xAt, yAt, Math.max(2, Math.min(11, stride * 0.62)), start, end);
      c.restore();

      if (this.entry !== null && this.entry >= min && this.entry <= max) {
        const ey = yAt(this.entry);
        c.save(); c.setLineDash([6, 4]); c.strokeStyle = P.accent; c.lineWidth = 1;
        c.beginPath(); c.moveTo(left, ey); c.lineTo(left + width, ey); c.stroke(); c.restore();
        c.fillStyle = P.accent; c.font = `700 8px ${P.mono}`; c.textAlign = "left"; c.textBaseline = "bottom";
        c.fillText(`ENTRY ${fmt(this.entry)}`, left + 6, ey - 3);
      }

      const nearest = (ts) => {
        let best = start, bestD = Infinity;
        for (let i = start; i < end; i += 1) { const d = Math.abs(all[i].timestamp - ts); if (d < bestD) { bestD = d; best = i; } }
        return best;
      };
      c.save();
      c.beginPath(); c.rect(left, top, width, height); c.clip();
      for (const m of marks) this.drawMark(tick ? xAt(m.timestamp) : xAt(nearest(m.timestamp)), yAt(m.price), m, top, top + height);
      c.restore();

      if (lastShown) {
        c.save();
        c.beginPath(); c.setLineDash([3, 5]); c.strokeStyle = rising ? P.upLine : P.downLine;
        c.moveTo(left, lastY); c.lineTo(left + width, lastY); c.stroke(); c.setLineDash([]);
        c.font = `700 ${narrow ? 9.5 : 10}px ${P.mono}`;
        c.shadowColor = rising ? P.upLine : P.downLine; c.shadowBlur = 12;
        this.badge(fmt(last), lastY, rising ? P.up : P.down, W, right, P.badgeInk);
        c.restore();
      }

      if (this.hover !== null && this.hover < n && (tick ? t[this.hover] >= t0 && t[this.hover] <= t1
                                                     : this.hover >= start && this.hover < end)) {
        const hx = tick ? xAt(t[this.hover]) : xAt(this.hover);
        const hp = tick ? p[this.hover] : all[this.hover].close;
        c.save(); c.setLineDash([3, 5]); c.strokeStyle = P.cross;
        c.beginPath(); c.moveTo(hx, top); c.lineTo(hx, top + height);
        const hy = this.hoverY ?? yAt(hp);
        c.moveTo(left, hy); c.lineTo(left + width, hy); c.stroke(); c.setLineDash([]);
        c.beginPath(); c.fillStyle = P.crossDot; c.arc(hx, yAt(hp), 3, 0, Math.PI * 2); c.fill();
        c.restore();
      }
      c.restore();
    }

    // HH above the tick, LL below; the tip touches the tick's price. IN/OUT a ringed dot.
    // CONF: a short white dash on the tick that confirmed a pivot.
    drawMark(x, y, m, top, bottom) {
      const c = this.ctx;
      c.save();
      c.font = `700 9px ${P.mono}`; c.textAlign = "center"; c.textBaseline = "middle";
      if (m.kind === "CONF") {
        c.strokeStyle = "#fff"; c.lineWidth = 2;
        c.beginPath(); c.moveTo(x - 5, y); c.lineTo(x + 5, y); c.stroke();
      } else if (m.kind === "HH" || m.kind === "LL") {
        const high = m.kind === "HH", colour = high ? P.down : P.up;
        const tip = y, base = high ? y - 9 : y + 9;
        c.fillStyle = colour; c.shadowColor = high ? P.downLine : P.upLine; c.shadowBlur = 8;
        c.beginPath(); c.moveTo(x, tip); c.lineTo(x - 5, base); c.lineTo(x + 5, base); c.closePath(); c.fill();
        c.shadowBlur = 0;
        c.fillText(m.kind, x, Math.max(top + 6, Math.min(bottom - 6, high ? base - 8 : base + 8)));
      } else {
        const entry = m.kind === "IN";
        c.beginPath(); c.arc(x, y, 4.5, 0, Math.PI * 2);
        c.fillStyle = P.badgeInk; c.fill();
        c.strokeStyle = P.accent; c.lineWidth = 1.5;
        if (entry) { c.fillStyle = P.accent; c.fill(); }
        c.stroke();
        c.fillStyle = P.accent;
        c.fillText(m.kind, x, Math.max(top + 6, y - 11));
      }
      c.restore();
    }

    drawTicks(xAt, yAt, top, height, start, end) {
      const c = this.ctx, { t, p } = this.ticks;
      const line = new Path2D();
      let firstX = null, lastX = null;
      if (end - start <= this.geom.width * 2) {
        for (let i = start; i < end; i += 1) {
          const x = xAt(t[i]), y = yAt(p[i]);
          if (firstX === null) { line.moveTo(x, y); firstX = x; } else line.lineTo(x, y);
          lastX = x;
        }
      } else {
        // More ticks than pixels: per column, enter at its first tick, sweep
        // its low and high, leave at its last - every price stays drawn.
        let col = null, lo = 0, hi = 0, first = 0, close = 0;
        const flush = () => {
          if (col === null) return;
          if (firstX === null) { line.moveTo(col, yAt(first)); firstX = col; } else line.lineTo(col, yAt(first));
          line.lineTo(col, yAt(lo)); line.lineTo(col, yAt(hi)); line.lineTo(col, yAt(close));
          lastX = col;
        };
        for (let i = start; i < end; i += 1) {
          const x = Math.round(xAt(t[i])) + 0.5, price = p[i];
          if (x !== col) { flush(); col = x; lo = hi = first = close = price; continue; }
          if (price < lo) lo = price; if (price > hi) hi = price; close = price;
        }
        flush();
      }
      const area = new Path2D(line);
      area.lineTo(lastX, top + height); area.lineTo(firstX, top + height); area.closePath();
      const rising = p[end - 1] >= p[start];
      const fill = c.createLinearGradient(0, top, 0, top + height);
      fill.addColorStop(0, rising ? P.upSoft : "rgba(255,96,124,.13)"); fill.addColorStop(1, "rgba(0,0,0,0)");
      c.fillStyle = fill; c.fill(area);
      c.strokeStyle = rising ? P.up : P.down; c.lineWidth = 1.25; c.lineJoin = "round";
      c.stroke(line);
    }

    drawBars(xAt, yAt, bodyW, start, end) {
      const c = this.ctx;
      for (let i = start; i < end; i += 1) {
        const k = this.bars[i], x = xAt(i), up = k.close >= k.open;
        c.beginPath(); c.strokeStyle = up ? P.up : P.down; c.lineWidth = 1;
        c.moveTo(Math.round(x) + 0.5, yAt(k.high)); c.lineTo(Math.round(x) + 0.5, yAt(k.low)); c.stroke();
        const oy = yAt(k.open), cy = yAt(k.close);
        c.fillStyle = up ? P.up : P.down;
        c.fillRect(x - bodyW / 2, Math.min(oy, cy), bodyW, Math.max(1.5, Math.abs(cy - oy)));
      }
    }
  }

  // ── State ────────────────────────────────────────────────────────────────
  const INDICES = (body.dataset.indices || "").split(",").map((s) => s.trim()).filter(Boolean);
  const remembered = store.get("lune.index");
  let index = INDICES.includes(remembered) ? remembered : (body.dataset.index || INDICES[0] || "");
  // The TICK chart's window, in minutes ("day": the whole session).
  const WINDOWS = ["5", "15", "30", "60", "180", "day"];
  let tickWindow = WINDOWS.includes(store.get("lune.tickWindow")) ? store.get("lune.tickWindow") : "5";
  let ticks = { t: [], p: [] };   // this index's session, every tick (and its high/low: `spanOf`)

  // The session's high and low, carried as ticks arrive (from index `from` on), so the
  // readout never rescans a whole day of ticks on every tick.
  function spanOf(series, from) {
    if (!Number.isFinite(series.lo) || !Number.isFinite(series.hi)) from = 0;
    let lo = from ? series.lo : Infinity, hi = from ? series.hi : -Infinity;
    for (let k = from; k < series.p.length; k += 1) { const x = series.p[k]; if (x < lo) lo = x; if (x > hi) hi = x; }
    series.lo = lo; series.hi = hi;
    return series;
  }
  let held = null;                // the day book's open position (`lune:books`), for its entry line
  let active = false;             // the HH/LL chart is the one on screen

  const chart = new DeskChart($("deskPlot"));
  chart.window = tickWindow === "day" ? null : Number(tickWindow) * 60e3;
  chart.resetView();
  chart.onSelect = (i, hovered) => {
    const kind = $("deskReadKind"), at = $("deskReadAt"), cells = [$("deskO"), $("deskH"), $("deskL"), $("deskC")];
    if (i === null) { at.textContent = "—"; cells.forEach((cell) => { cell.textContent = "—"; }); return; }
    if (chart.kind === "tick") {
      const { t, p } = chart.ticks;
      if (!Number.isFinite(chart.ticks.hi)) spanOf(chart.ticks, 0);
      const lo = chart.ticks.lo, hi = chart.ticks.hi;
      kind.textContent = hovered ? "TICK" : "LATEST TICK";
      at.textContent = fClock.format(new Date(t[i]));
      [p[0], hi, lo, p[i]].forEach((value, k) => { cells[k].textContent = fmt(value); });
      cells[3].classList.remove("up", "down"); cells[3].classList.add(p[i] >= p[0] ? "up" : "down");
    } else {
      const k = chart.bars[i];
      kind.textContent = hovered ? "BAR" : "LATEST BAR";
      at.textContent = fHm.format(new Date(k.timestamp));
      [k.open, k.high, k.low, k.close].forEach((value, n) => { cells[n].textContent = fmt(value); });
      cells[3].classList.remove("up", "down"); cells[3].classList.add(k.close >= k.open ? "up" : "down");
    }
  };

  // The header's feed pod is the algo console's; this chart reports through its veil only.
  const setFeed = () => {};

  function setVeil(state, title, detail) {
    const veil = $("deskVeil");
    veil.dataset.state = state;
    veil.querySelector("b").textContent = title;
    veil.querySelector("small").textContent = detail;
    veil.classList.toggle("is-gone", state === "hidden");
  }

  // A pivot sits on the exact tick that made it: `extreme_at` is that tick's epoch ms,
  // the same clock as the tick stream. The ISO time is only a fallback (whole seconds).
  function normalizePivots(points, kind) {
    return (Array.isArray(points) ? points : []).map((p) => {
      const price = num(p?.price ?? p?.[kind === "HH" ? "high" : "low"]);
      const at = Number(p?.extreme_at);
      const timestamp = Number.isFinite(at) && at > 0 ? at : parseFeedTime(p?.time);
      return price === null || !Number.isFinite(timestamp) ? null : {
        kind, price, timestamp, confirmedAt: Number(p?.confirmed_at), confirmedPrice: num(p?.confirmed_price),
      };
    }).filter(Boolean);
  }

  // The desk's HH/LL pivots for this index: the algo's detector, on this tape.
  let pivotList = [], pivotsBusy = false;
  async function loadPivots() {
    if (!active || !index || pivotsBusy) return;
    const wanted = index;
    pivotsBusy = true;
    try {
      const data = await fetchJson(`/manual/pivots?index=${encodeURIComponent(wanted)}`, 8000);
      if (wanted !== index) return;
      pivotList = [...normalizePivots(data?.hh_points, "HH"), ...normalizePivots(data?.ll_points, "LL")];
      draw();
    } catch (_e) { /* keeps the last pivots until the next poll */ }
    finally { pivotsBusy = false; }
  }

  // The HH/LL pivots and their confirm dashes; no IN / OUT trade marks.
  function deskMarks() {
    return pivotList.flatMap((p) => (
      p.confirmedAt > 0 && p.confirmedPrice !== null
        ? [p, { kind: "CONF", price: p.confirmedPrice, timestamp: p.confirmedAt }] : [p]));
  }

  function draw() {
    const entry = held && held.index === index ? num(held.entry) : null;
    chart.setMarks(deskMarks(), 0);
    chart.set("tick", ticks, entry);
    $("deskCount").textContent = `${nf0.format(ticks.t.length)} ticks`;
    $("deskChartMeta").textContent = ticks.t.length
      ? `EVERY TICK · ${fHm.format(new Date(ticks.t[0]))}–${fHm.format(new Date(ticks.t[ticks.t.length - 1]))} IST`
      : "EVERY TICK · SESSION";
    if (ticks.t.length) setVeil("hidden", "", "");
    else setVeil("waiting", "Waiting for ticks", `Every ${index} tick is drawn from the first one this session.`);
  }


  // ── Feeds ────────────────────────────────────────────────────────────────
  let stream = null;
  // The id of the last stream message applied to `ticks`: a reopened stream goes on from it
  // instead of resending the whole session (the server sends everything when it cannot).
  let streamId = null;

  // A message's ticks: plain {t, p}, or compact (enc "d1": the first time and the steps after
  // it; prices as integers of 1/ps, the same floats the server holds).
  function decodeTicks(m) {
    if (m.enc !== "d1") return { t: Array.isArray(m.t) ? m.t : [], p: Array.isArray(m.p) ? m.p : [] };
    const t = [], p = [], dt = Array.isArray(m.dt) ? m.dt : [];
    if (!Number.isFinite(m.t0)) return { t, p };
    let at = m.t0;
    t.push(at);
    for (let i = 0; i < dt.length; i += 1) { at += dt[i]; t.push(at); }
    if (Number.isFinite(m.ps) && Number.isFinite(m.q0)) {
      const dq = Array.isArray(m.dq) ? m.dq : [];
      let q = m.q0;
      p.push(q / m.ps);
      for (let i = 0; i < dq.length; i += 1) { q += dq[i]; p.push(q / m.ps); }
    } else if (Array.isArray(m.p)) p.push(...m.p);
    return t.length === p.length ? { t, p } : { t: [], p: [] };
  }

  function openStream() {
    closeStream();
    if (!active || !index || document.hidden) return;
    const query = new URLSearchParams({ index, enc: "d1" });
    if (streamId && ticks.t.length) query.set("from", streamId);
    const source = stream = new EventSource(`/manual/tick_stream?${query}`);
    source.onopen = () => { if (source === stream) setFeed("live", "Live"); };
    source.onerror = () => {
      if (source !== stream) return;
      setFeed("error", "Delayed");
      if (!ticks.t.length) setVeil("error", "Tick feed unavailable", "The desk reconnects by itself.");
    };
    source.onmessage = (event) => {
      if (source !== stream) return;
      let message;
      try { message = JSON.parse(event.data); } catch (_e) { return; }
      if (!message || message.index !== index) return;
      const got = decodeTicks(message);
      if (message.reset) ticks = spanOf({ t: got.t, p: got.p }, 0);
      else {
        // Not where this chart left off (a missed message): start the session over.
        if (Number.isFinite(message.from) && message.from !== ticks.t.length) { streamId = null; openStream(); return; }
        const from = ticks.t.length;
        for (let i = 0; i < got.t.length; i += 1) { ticks.t.push(got.t[i]); ticks.p.push(got.p[i]); }
        spanOf(ticks, from);
      }
      streamId = event.lastEventId || null;
      setFeed("live", "Live");
      draw();
    };
  }
  function closeStream() { if (stream) stream.close(); stream = null; }

  // ── Wiring ───────────────────────────────────────────────────────────────
  // V3 or HH/LL: one chart on screen at a time, the choice remembered.
  const MODES = ["v3", "tick"];
  let mode = MODES.includes(store.get("lune.chart")) ? store.get("lune.chart") : "v3";

  function setMode(next) {
    mode = MODES.includes(next) ? next : "v3";
    store.set("lune.chart", mode);
    const tick = mode === "tick";
    for (const node of document.querySelectorAll("#market > .chart-surface, #readout, #chartLegend")) node.hidden = tick;
    $("tickView").hidden = !tick;
    for (const button of document.querySelectorAll("#chartModes [data-chart]"))
      button.setAttribute("aria-pressed", String(button.dataset.chart === mode));
    if (tick) start(); else stop();
    // The canvas just shown measures itself against its new box.
    window.dispatchEvent(new Event("resize"));
  }

  function selectWindow(next) {
    if (!WINDOWS.includes(next)) return;
    tickWindow = next;
    store.set("lune.tickWindow", next);
    for (const button of document.querySelectorAll("#deskWindows [data-window]"))
      button.setAttribute("aria-pressed", String(button.dataset.window === next));
    chart.window = next === "day" ? null : Number(next) * 60e3;
    chart.resetView();
    chart.schedule();
  }

  function selectIndex(next) {
    if (!next || next === index) return;
    index = next;
    ticks = { t: [], p: [] }; pivotList = []; streamId = null;
    draw();
    if (active) { openStream(); loadPivots(); }
  }

  function start() {
    if (active) return;
    active = true;
    openStream(); loadPivots(); draw();
  }

  function stop() {
    active = false;
    closeStream();
  }

  function every(task, interval) {
    const run = async () => {
      if (active && !document.hidden) await task();
      window.setTimeout(run, interval);
    };
    window.setTimeout(run, interval);
  }

  $("chartModes").addEventListener("click", (event) => {
    const button = event.target.closest("[data-chart]");
    if (button && button.dataset.chart !== mode) setMode(button.dataset.chart);
  });
  $("deskWindows").addEventListener("click", (event) => {
    const button = event.target.closest("[data-window]");
    if (button) selectWindow(button.dataset.window);
  });
  document.addEventListener("lune:index", (event) => selectIndex(event.detail));
  // The day position's entry line, from the console's own read of the books.
  document.addEventListener("lune:books", (event) => {
    const next = event.detail?.day?.position || null;
    if (JSON.stringify(next && [next.index, next.entry]) === JSON.stringify(held && [held.index, held.entry])) return;
    held = next;
    if (active) draw();
  });
  document.addEventListener("visibilitychange", () => {
    if (!active) return;
    if (document.hidden) { closeStream(); return; }
    openStream(); loadPivots();
  });
  window.addEventListener("resize", () => { if (active) chart.schedule(); });

  for (const button of document.querySelectorAll("#deskWindows [data-window]"))
    button.setAttribute("aria-pressed", String(button.dataset.window === tickWindow));
  setMode(mode);
  // Pivots print as the engine confirms them.
  every(loadPivots, 1000);
})();
