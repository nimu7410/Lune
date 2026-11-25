/* Lune · tick backtester. Vanilla, no dependencies, like Lune's own console. The chart is
   Lune's manual-desk DeskChart, copied unchanged (static/manual.js), so a replayed session
   reads exactly as a live one does; its palette is read out of Lune-v5.css. */
(() => {
  "use strict";

  const body = document.body;
  const $ = (id) => document.getElementById(id);

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
  const fDayShort = new Intl.DateTimeFormat("en-GB", { timeZone: "UTC", day: "2-digit", month: "short" });
  const fWeekday = new Intl.DateTimeFormat("en-GB", { timeZone: "UTC", weekday: "short" });
  const fmt = (x) => (Number.isFinite(x) ? nf.format(x) : "—");
  const signed = (x, s = "") => (Number.isFinite(x) ? `${x > 0 ? "+" : ""}${nf.format(x)}${s}` : "—");
  const rupees = (x) => (Number.isFinite(x) ? `${x > 0 ? "+" : x < 0 ? "−" : ""}₹${nf0.format(Math.abs(x))}` : "—");
  const plain = (x) => (Number.isFinite(x) ? `₹${nf0.format(x)}` : "—");
  const dayLabel = (iso) => fDayShort.format(new Date(`${iso}T00:00:00Z`)).toUpperCase();
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  function tone(node, value) {
    node.classList.remove("up", "down", "flat");
    node.classList.add(value > 0 ? "up" : value < 0 ? "down" : "flat");
  }

  async function api(path, payload, timeoutMs = 120000) {
    const ac = new AbortController();
    const timer = window.setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(path, {
        method: payload === undefined ? "GET" : "POST", cache: "no-store", signal: ac.signal,
        headers: { Accept: "application/json", ...(payload === undefined ? {} : { "Content-Type": "application/json" }) },
        body: payload === undefined ? undefined : JSON.stringify(payload),
      });
      let data = null;
      try { data = await res.json(); } catch (_e) { /* not JSON */ }
      if (!res.ok) throw new Error(data?.error || `${path}: HTTP ${res.status}`);
      return data;
    } catch (e) {
      throw e.name === "AbortError" ? new Error(`${path}: timed out`) : e;
    } finally { window.clearTimeout(timer); }
  }

  let toastTimer = 0;
  function toast(message) {
    const node = $("toast");
    node.textContent = message;
    node.classList.add("is-visible");
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => node.classList.remove("is-visible"), 4200);
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
      this.marks = []; this.barMs = 0;   // HH/LL pivots and their confirmations, by timestamp
      this.boxes = [];                   // needles: { conf_ms, conf_px, end_ms, end_px }, shaded under the price
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
    setMarks(marks, barMs, boxes = []) { this.marks = marks; this.barMs = barMs; this.boxes = boxes; this.schedule(); }

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
      if (this.boxes.length) {
        if (!tick) {                          // candles: the bar a timestamp falls in
          const stamps = all.map((k) => k.timestamp);
          xAt.bar = (ts) => clamp(lowerBound(stamps, ts + 1) - 1, 0, n - 1);
        }
        this.drawNeedles(xAt, yAt, tick, stride, tick ? t0 : all[start].timestamp, tick ? t1 : all[end - 1].timestamp + this.barMs);
      }
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

    // HH above the tick, LL below; the tip touches the tick's price.
    // CONF: a short white dash on the tick that confirmed a pivot.
    drawMark(x, y, m, top, bottom) {
      const c = this.ctx;
      c.save();
      c.font = `700 9px ${P.mono}`; c.textAlign = "center"; c.textBaseline = "middle";
      if (m.kind === "CONF") {
        c.strokeStyle = "#fff"; c.lineWidth = 2;
        c.beginPath(); c.moveTo(x - 5, y); c.lineTo(x + 5, y); c.stroke();
      } else {
        const high = m.kind === "HH", colour = high ? P.down : P.up;
        const tip = y, base = high ? y - 9 : y + 9;
        c.fillStyle = colour; c.shadowColor = high ? P.downLine : P.upLine; c.shadowBlur = 8;
        c.beginPath(); c.moveTo(x, tip); c.lineTo(x - 5, base); c.lineTo(x + 5, base); c.closePath(); c.fill();
        c.shadowBlur = 0;
        c.fillText(m.kind, x, Math.max(top + 6, Math.min(bottom - 6, high ? base - 8 : base + 8)));
      }
      c.restore();
    }

    // A needle: the area between the price and its dash price (the entry), from the dash to
    // the tick that ended it, greyed out translucently. Candles: through each bar's close.
    drawNeedles(xAt, yAt, tick, stride, from, to) {
      const c = this.ctx;
      c.save();
      c.globalAlpha = 0.28; c.fillStyle = "#a8acb3";
      for (const b of this.boxes) {
        if (b.end_ms < from || b.conf_ms > to) continue;
        const area = new Path2D(), base = yAt(b.conf_px);
        if (tick) {
          const { t, p } = this.ticks;
          const i0 = lowerBound(t, b.conf_ms), i1 = Math.max(i0, lowerBound(t, b.end_ms + 1) - 1);
          area.moveTo(xAt(t[i0]), base);
          for (let i = i0; i <= i1; i += 1) area.lineTo(xAt(t[i]), yAt(p[i]));
          area.lineTo(xAt(t[i1]), base);
        } else {
          const i0 = xAt.bar(b.conf_ms), i1 = Math.max(i0, xAt.bar(b.end_ms));
          area.moveTo(xAt(i0) - stride / 2, base);
          for (let i = i0; i <= i1; i += 1) area.lineTo(xAt(i), yAt(i === i1 ? b.end_px : this.bars[i].close));
          area.lineTo(xAt(i1) + stride / 2, yAt(b.end_px));
          area.lineTo(xAt(i1) + stride / 2, base);
        }
        area.closePath();
        c.fill(area);
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
  const INDICES = (body.dataset.indices || "").split(",").filter(Boolean);
  const NUMERIC = ["latency_ms", "lots", "limit_wait", "leg_k", "chop_k", "exit_wide", "stop", "target", "lock_at", "lock_trail", "max_hold_s", "needle_arm", "needle_give", "needle_wait_s"];
  const TEXT = ["fill", "order", "last_entry", "exit_on", "needle"];
  const NEEDLE_PRESET = {   // each rule's settings when it is picked (the tested defaults)
    giveback: { needle_arm: 1, needle_give: 0.5 }, entry: { needle_arm: 2 },
    stall: { needle_arm: 1, needle_give: 0.7, needle_wait_s: 1800 },
  };
  const NEEDLE_NAME = { giveback: "give back", entry: "back to entry", stall: "give back + stall" };
  const PIVOT_POS = { R: 0, threshold: 1, min_leg: 2 };
  const LABEL = {
    R: "R ×cost", threshold: "Threshold ×cost", min_leg: "Min leg ×cost", chop_k: "Chop filter ×R", leg_k: "Leg filter ×R",
    lock_at: "Lock at ×R", lock_trail: "Lock trail ×R", stop: "Stop ×R", target: "Target ×R", exit_on: "Exit on",
    exit_wide: "Wide zigzag ×R", max_hold_s: "Max hold s", last_entry: "Last entry", order: "Entry order", limit_wait: "Limit wait s",
    needle: "Needle rule", needle_arm: "Needle arm ×R", needle_give: "Needle give back", needle_wait_s: "Needle wait s",
  };
  const SETTINGS_KEY = "bt.settings.scaled";   // renamed when the default changed to scaled legs
  let META = null;
  let S = null;                    // the settings, in the shape the API takes
  let index = INDICES.includes(store.get("bt.index")) ? store.get("bt.index") : (INDICES.includes("NIFTY") ? "NIFTY" : INDICES[0]);
  let day = store.get("bt.day");
  let frame = "tick";
  let tickWindow = "day";
  let replay = null;               // the charted session: ticks, pivots, trades
  let lastRun = null;              // the last RUN: { stats, trades, days, settings }
  let focus = null;                // a trade to zoom to once its session is charted

  const chart = new DeskChart($("plot"));
  chart.window = null;
  chart.resetView();
  chart.onSelect = (i, hovered) => {
    const kind = $("deskReadKind"), at = $("deskReadAt"), cells = [$("deskO"), $("deskH"), $("deskL"), $("deskC")];
    if (i === null) { at.textContent = "—"; cells.forEach((cell) => { cell.textContent = "—"; }); return; }
    if (chart.kind === "tick") {
      const { t, p } = chart.ticks;
      let lo = Infinity, hi = -Infinity;
      for (let k = 0; k < p.length; k += 1) { if (p[k] < lo) lo = p[k]; if (p[k] > hi) hi = p[k]; }
      kind.textContent = hovered ? "TICK" : "LAST TICK";
      at.textContent = fClock.format(new Date(t[i]));
      [p[0], hi, lo, p[i]].forEach((value, k) => { cells[k].textContent = fmt(value); });
      cells[3].classList.remove("up", "down"); cells[3].classList.add(p[i] >= p[0] ? "up" : "down");
    } else {
      const k = chart.bars[i];
      kind.textContent = hovered ? "BAR" : "LAST BAR";
      at.textContent = fHm.format(new Date(k.timestamp));
      [k.open, k.high, k.low, k.close].forEach((value, n) => { cells[n].textContent = fmt(value); });
      cells[3].classList.remove("up", "down"); cells[3].classList.add(k.close >= k.open ? "up" : "down");
    }
  };

  function setVeil(state, title, detail) {
    const veil = $("deskVeil");
    veil.dataset.state = state;
    veil.querySelector("b").textContent = title;
    veil.querySelector("small").textContent = detail;
    veil.classList.toggle("is-gone", state === "hidden");
  }

  function setStatus(node, toneName, text) {
    node.dataset.tone = toneName;
    node.textContent = text;
  }

  // ── Settings ─────────────────────────────────────────────────────────────
  function validSettings(x) {
    if (!x || typeof x !== "object" || !x.pivots) return false;
    if (![...NUMERIC, ...TEXT].every((k) => k in x)) return false;
    return INDICES.every((n) => Array.isArray(x.pivots[n]) && x.pivots[n].length === 3 && x.pivots[n].every(Number.isFinite));
  }

  function saveSettings() { store.set(SETTINGS_KEY, JSON.stringify(S)); }

  function fillForm() {
    const form = $("settingsFormMain");
    for (const el of form.elements) {
      if (!el.name || !(el.name in S)) continue;
      el.value = S[el.name];
      el.closest("label")?.removeAttribute("data-bad");
      markOff(el);
    }
    form.elements.needle_arm.disabled = S.needle === "off";       // only what the chosen rule reads
    form.elements.needle_give.disabled = !["giveback", "stall"].includes(S.needle);
    form.elements.needle_wait_s.disabled = S.needle !== "stall";
    renderPivotSummary();
    $("ticketMeta").textContent = S.fill === "next"
      ? `FILL NEXT TICK${S.latency_ms > 0 ? ` +${nf0.format(S.latency_ms)} MS` : ""} · ${S.lots} LOT${S.lots > 1 ? "S" : ""}`
      : `FILL SAME TICK (LUNE PAPER) · ${S.lots} LOT${S.lots > 1 ? "S" : ""}`;
    $("runFill").textContent = S.fill === "next" ? "next-tick fills" : "same-tick fills";
  }

  function markOff(el) {
    const zeroOff = ["leg_k", "chop_k", "exit_wide", "stop", "target", "lock_at", "max_hold_s"].includes(el.name);
    el.closest("label")?.toggleAttribute("data-off", zeroOff && Number(el.value) === 0);
  }

  function readField(el) {
    const label = el.closest("label");
    if (NUMERIC.includes(el.name)) {
      const x = el.value.trim() === "" ? NaN : Number(el.value);
      const min = el.min === "" ? -Infinity : Number(el.min), max = el.max === "" ? Infinity : Number(el.max);
      const whole = el.name === "lots";
      if (!Number.isFinite(x) || x < min || x > max || (whole && !Number.isInteger(x))) {
        label?.setAttribute("data-bad", ""); return false;
      }
      S[el.name] = x;
    } else {
      if (el.name === "last_entry") {
        const m = el.value.trim().match(/^(\d{1,2}):(\d{2})$/), s = m ? Number(m[1]) * 3600 + Number(m[2]) * 60 : NaN;
        if (!m || Number(m[2]) > 59 || !(s >= 33300 && s <= 54000)) { label?.setAttribute("data-bad", ""); return false; }
        S[el.name] = `${m[1].padStart(2, "0")}:${m[2]}`;
        label?.removeAttribute("data-bad");
        return true;
      }
      S[el.name] = el.value;
    }
    label?.removeAttribute("data-bad");
    markOff(el);
    return true;
  }

  const costOf = (n) => META?.cost_pts?.[n];
  const tickOf = (n) => META?.tick_size?.[n] || 0.05;
  // x cost -> points, as the engine rounds it (to the tick, at least one tick; 0 stays 0)
  const pts = (n, k) => {
    const c = costOf(n), t = tickOf(n);
    if (!Number.isFinite(c) || !k) return k ? null : 0;
    return Math.max(t, Math.round((k * c) / t) * t);
  };

  function renderPivotSummary() {
    const [R, thr, leg] = S.pivots[index];
    const row = (label, k, note = "") => {
      const p = pts(index, k);
      return `<div><dt>${label}</dt><dd>${k ? `${fmt(k)}× <small>· ${p === null ? "—" : `${fmt(p)} pts`}</small>` : `<small>${note}</small>`}</dd></div>`;
    };
    $("pivotSummary").innerHTML =
      `<div><dt>${esc(index)} · 1× COST</dt><dd>${Number.isFinite(costOf(index)) ? `${fmt(costOf(index))} pts` : "—"}</dd></div>` +
      row("R", R) + row("THRESHOLD", thr, "0 · R prints pivots") + row("MIN LEG", leg, "off") +
      `<div><dt>NEEDLE</dt><dd>${S.needle !== "off" ? `${NEEDLE_NAME[S.needle] || esc(S.needle)} <small>· arm ${fmt(S.needle_arm)}× R${S.needle !== "entry" ? ` · ${Math.round(S.needle_give * 100)}%` : ""}${S.needle === "stall" ? ` · ${nf0.format(S.needle_wait_s)} s` : ""}</small>` : "<small>off</small>"}</dd></div>`;
  }

  function openPivots() {
    $("settingsCostNote").textContent = META?.days?.length
      ? `Points use each index's cost at the open of ${META.days[META.days.length - 1]} (it moves with price and days to expiry).`
      : "No sessions yet.";
    $("settingsRows").innerHTML = INDICES.map((n) => {
      const [R, thr, leg] = S.pivots[n];
      const cell = (k, v, label) => `<td data-label="${label}"><input type="number" min="${k === 0 ? 0.05 : 0}" step="0.5" data-index="${esc(n)}" data-k="${k}" value="${v}" aria-label="${esc(n)} ${label}"></td>`;
      return `<tr><th scope="row">${esc(n)}</th>${cell(0, R, "R")}${cell(1, thr, "Threshold")}${cell(2, leg, "Min leg")}<td class="settings-now" data-now="${esc(n)}"></td></tr>`;
    }).join("");
    const now = () => {
      for (const n of INDICES) {
        const v = [0, 1, 2].map((k) => Number($("settingsRows").querySelector(`input[data-index="${n}"][data-k="${k}"]`).value));
        const f = (x) => (pts(n, x) === null ? "—" : fmt(pts(n, x)));
        $("settingsRows").querySelector(`[data-now="${n}"]`).textContent = `R ${f(v[0])} · T ${v[1] ? f(v[1]) : f(v[0])} · L ${v[2] ? f(v[2]) : "off"}`;
      }
    };
    $("settingsRows").oninput = now;
    for (const b of $("settings").querySelectorAll("[data-preset]")) b.onclick = () => {
      for (const el of $("settingsRows").querySelectorAll("input")) el.value = META[b.dataset.preset].pivots[el.dataset.index][Number(el.dataset.k)];
      now();
      setStatus($("settingsStatus"), "", `${b.textContent} filled in: Apply to use them.`);
    };
    now();
    setStatus($("settingsStatus"), "", "");
    $("settings").showModal();
  }

  function applyPivots(event) {
    event.preventDefault();
    const next = {};
    for (const n of INDICES) {
      const v = [0, 1, 2].map((k) => $("settingsRows").querySelector(`input[data-index="${n}"][data-k="${k}"]`));
      const x = v.map((el) => (el.value.trim() === "" ? NaN : Number(el.value)));
      if (!x.every(Number.isFinite) || x[0] <= 0 || x[1] < 0 || x[2] < 0) {
        setStatus($("settingsStatus"), "bad", `${n}: R must be above 0, threshold and min leg 0 or more.`);
        v[x.findIndex((y, k) => !Number.isFinite(y) || (k === 0 ? y <= 0 : y < 0))]?.focus();
        return;
      }
      next[n] = x;
    }
    S.pivots = next;
    saveSettings(); fillForm();
    $("settings").close();
    loadDay();
  }

  // ── The charted session ──────────────────────────────────────────────────
  function barsOf(minutes) {
    const step = minutes * 60000, out = [], { t, p } = replay.ticks;
    for (let i = 0; i < t.length; i += 1) {
      const start = Math.floor(t[i] / step) * step, last = out[out.length - 1];
      if (last && last.timestamp === start) {
        if (p[i] > last.high) last.high = p[i];
        if (p[i] < last.low) last.low = p[i];
        last.close = p[i];
      } else out.push({ timestamp: start, open: p[i], high: p[i], low: p[i], close: p[i] });
    }
    return out;
  }

  function marks() {
    if (!replay) return [];
    const out = [];
    for (const v of replay.pivots) {
      out.push({ kind: v.kind, price: v.price, timestamp: v.ms });
      out.push({ kind: "CONF", price: v.conf_price, timestamp: v.conf_ms });
    }
    return out;
  }

  function draw() {
    if (!replay) return;
    chart.setMarks(marks(), frame === "tick" ? 0 : Number(frame) * 60e3, replay.needles || []);
    const { t } = replay.ticks;
    if (frame === "tick") {
      chart.set("tick", replay.ticks, null);
      $("deskCount").textContent = `${nf0.format(t.length)} ticks`;
      $("deskChartMeta").textContent = t.length ? `EVERY TICK · ${fHm.format(new Date(t[0]))}–${fHm.format(new Date(t[t.length - 1]))} IST` : "—";
    } else {
      const series = barsOf(Number(frame));
      chart.set("bars", series, null);
      $("deskCount").textContent = `${series.length} bars`;
      $("deskChartMeta").textContent = `${frame}M OHLC FROM TICKS · ${replay.day}`;
    }
    if (focus && frame === "tick" && focus.index === replay.index && focus.day === replay.day) {
      const span = Math.max(120e3, (focus.exit_ms - focus.entry_ms) * 3);
      chart.tSpan = span;
      chart.tTo = Math.min(t[t.length - 1], focus.exit_ms + span / 3);
      chart.schedule();
      focus = null;
    }
  }

  // The session's legs (pivot to pivot) and how long each pivot took to print, in one line.
  function legLine(pivots, minLeg) {
    const mid = (xs) => { const s = [...xs].sort((a, b) => a - b), h = s.length >> 1; return s.length % 2 ? s[h] : (s[h - 1] + s[h]) / 2; };
    const dur = (x) => { const s = Math.round(x); return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, "0")}s`; };
    if (!pivots.length) return "no pivots";
    const legs = pivots.filter((v) => v.leg !== null).map((v) => v.leg), lag = pivots.map((v) => (v.conf_ms - v.ms) / 1000);
    const forced = pivots.filter((v) => v.forced).length;
    const full = minLeg ? ` · ${legs.filter((x) => x >= minLeg - 1e-9).length} of ${legs.length} at least the min leg` : "";
    return `${legs.length ? `legs ${fmt(Math.min(...legs))} to ${fmt(Math.max(...legs))}, median ${fmt(mid(legs))} pts${full} · ` : ""}` +
      `confirmed in ${dur(mid(lag))} median, ${dur(Math.max(...lag))} at most · ${forced ? `${forced} forced` : "none forced"}`;
  }

  let dayRequest = 0, dayTimer = 0;
  function loadDaySoon() { window.clearTimeout(dayTimer); dayTimer = window.setTimeout(loadDay, 300); }

  async function loadDay() {
    window.clearTimeout(dayTimer);
    if (!META || !day || !index) return;
    const request = ++dayRequest, wanted = { index, day };
    $("deskIndex").textContent = index;
    $("deskDay").textContent = `${day} · ${fWeekday.format(new Date(`${day}T00:00:00Z`)).toUpperCase()}`;
    if (!META.files.some((f) => f.day === day && f.index === index)) {
      replay = null; chart.set("tick", { t: [], p: [] }, null);
      setVeil("error", "No ticks for this session", `${index} has no stored ticks on ${day}.`);
      $("dayNet").textContent = "—"; $("dayMeta").textContent = "No ticks"; $("dayPivots").textContent = "—"; $("dayLegs").textContent = "—";
      return;
    }
    setVeil("waiting", "Replaying the session", `Every ${index} tick of ${day}, through the HH/LL engine.`);
    try {
      const d = await api("/api/day", { settings: S, index, day });
      if (request !== dayRequest || wanted.index !== index || wanted.day !== day) return;
      const t = new Array(d.dt.length);
      let at = d.t0;
      for (let i = 0; i < d.dt.length; i += 1) { at += d.dt[i]; t[i] = at; }
      replay = { ...d, ticks: { t, p: d.p } };
      setVeil("hidden", "", "");
      const net = $("dayNet");
      net.textContent = rupees(d.net); tone(net, d.net);
      const wins = d.trades.filter((x) => x.net_rs > 0).length;
      $("dayMeta").textContent = `${d.trades.length} trade${d.trades.length === 1 ? "" : "s"} · ${wins} won · ${d.pivots.length} pivots`;
      tone($("dayMeta"), d.net);
      const blocked = Object.entries(d.blocked).filter(([, v]) => v > 0).map(([k, v]) => `${k} ${v}`).join(", ");
      $("dayLegs").textContent = legLine(d.pivots, d.leg);
      $("dayPivots").textContent = `R ${fmt(d.R)} · threshold ${fmt(d.threshold)} · min leg ${d.leg ? fmt(d.leg) : "off"} pts · cost ${fmt(d.cost)}${blocked ? ` · blocked: ${blocked}` : ""}`;
      draw();
    } catch (e) {
      if (request !== dayRequest) return;
      setVeil("error", "Replay failed", e.message);
    }
  }

  // ── Pickers ──────────────────────────────────────────────────────────────
  function renderIndexPicker() {
    for (const chip of document.querySelectorAll("#indexPicker .picker-chip"))
      chip.setAttribute("aria-pressed", String(chip.dataset.index === index));
  }

  function dayNets() {
    if (!lastRun) return {};
    const out = {};
    for (const t of lastRun.trades) if (t.index === index) out[t.day] = (out[t.day] || 0) + t.net_rs;
    return out;
  }

  function renderDayPicker() {
    const nets = dayNets(), ran = lastRun && (!lastRun.index || lastRun.index === index);
    $("dayPicker").innerHTML = (META?.days || []).map((d) => {
      const n = nets[d] || 0, has = META.files.some((f) => f.day === d && f.index === index);
      const small = !has ? "NO TICKS" : ran && lastRun.days.includes(d) ? rupees(n) : fWeekday.format(new Date(`${d}T00:00:00Z`)).toUpperCase();
      return `<button type="button" data-day="${d}" aria-pressed="${d === day}" data-net="${ran && n ? (n > 0 ? "up" : "down") : ""}"${has ? "" : " disabled"}>${dayLabel(d)}<small class="tnum">${small}</small></button>`;
    }).join("");
    $("dayPicker").querySelector('[aria-pressed="true"]')?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }

  function selectIndex(next) {
    if (!INDICES.includes(next) || next === index) return;
    index = next; store.set("bt.index", index);
    renderIndexPicker(); renderDayPicker(); renderPivotSummary(); loadDay();
  }

  function selectDay(next) {
    if (!META.days.includes(next) || next === day) return;
    day = next; store.set("bt.day", day);
    renderDayPicker(); loadDay();
  }

  // ── Run ──────────────────────────────────────────────────────────────────
  function fillRange() {
    const opts = (META.days || []).map((d) => `<option value="${d}">${d}</option>`).join("");
    const from = $("fromDay").value, to = $("toDay").value;
    $("fromDay").innerHTML = opts; $("toDay").innerHTML = opts;
    $("fromDay").value = META.days.includes(from) ? from : META.days[0] || "";
    $("toDay").value = META.days.includes(to) ? to : META.days[META.days.length - 1] || "";
  }

  function range() {
    const from = $("fromDay").value, to = $("toDay").value;
    if (from > to) throw new Error("FROM is after TO.");
    return { from, to };
  }

  function rows(list, key) {
    const most = Math.max(1, ...list.map((r) => Math.abs(r.net)));
    return list.map((r) => `<tr><td>${esc(r[key])}</td><td>${r.trades}</td><td class="${r.net > 0 ? "up" : r.net < 0 ? "down" : "flat"}" style="--magnitude:${Math.round((Math.abs(r.net) / most) * 100)}%">${rupees(r.net)}</td></tr>`).join("");
  }

  async function run(event) {
    event?.preventDefault();
    let span;
    try { span = range(); } catch (e) { setStatus($("runStatus"), "bad", e.message); return; }
    const scope = $("runIndex").value;
    setStatus($("runStatus"), "work", "Replaying every session…");
    setStatus($("ticketStatus"), "work", "Replaying every session…");
    $("runButton").disabled = true; $("ticketTag").dataset.desk = "busy"; $("ticketTag").textContent = "Running";
    try {
      const r = await api("/api/run", { settings: S, ...span, index: scope });
      lastRun = { ...r, index: scope };
      renderRun();
      const msg = `${r.stats.trades} trades on ${r.days.length} session${r.days.length === 1 ? "" : "s"} (${r.days[0]} to ${r.days[r.days.length - 1]}), ${scope || "all six indices"}, in ${r.seconds}s.`;
      setStatus($("runStatus"), "", msg);
      setStatus($("ticketStatus"), r.stats.net >= 0 ? "ok" : "bad", `Net ${rupees(r.stats.net)} · ${msg}`);
      $("ticketTag").dataset.desk = "ready"; $("ticketTag").textContent = "Ran";
      loadDay();
    } catch (e) {
      setStatus($("runStatus"), "bad", e.message);
      setStatus($("ticketStatus"), "bad", e.message);
      $("ticketTag").dataset.desk = "error"; $("ticketTag").textContent = "Error";
    } finally { $("runButton").disabled = false; }
  }

  function renderRun() {
    const s = lastRun.stats;
    $("runResults").hidden = false;
    const put = (id, text, value) => { const n = $(id); n.textContent = text; if (value !== undefined) tone(n, value); };
    put("sNet", rupees(s.net), s.net);
    put("sTrades", nf0.format(s.trades));
    $("sWins").textContent = s.trades ? `${s.wins} WON · ${Math.round((s.wins / s.trades) * 100)}%` : "—";
    put("sPf", s.pf === null ? (s.trades ? "∞" : "—") : fmt(s.pf), s.pf === null ? 0 : s.pf - 1);
    put("sDd", plain(s.max_dd));
    put("sGross", rupees(s.gross), s.gross);
    put("sCharges", plain(s.charges));
    put("sAvg", s.avg === null ? "—" : rupees(s.avg), s.avg || 0);
    $("sHalves").innerHTML = `<span class="${s.h1 >= 0 ? "up" : "down"}">${rupees(s.h1)}</span> / <span class="${s.h2 >= 0 ? "up" : "down"}">${rupees(s.h2)}</span>`;
    $("dailyRows").innerHTML = rows(s.by_day, "day");
    $("symbolRows").innerHTML = rows(s.by_index, "index");
    $("reasonRows").innerHTML = rows(s.by_reason, "reason");
    $("tradeRows").innerHTML = lastRun.trades.map((t, i) => `<tr data-i="${i}" tabindex="0">` +
      `<td>${t.day}</td><td>${esc(t.index)}</td><td class="${t.side === "LONG" ? "long" : "short"}">${t.side}</td>` +
      `<td>${t.entry}</td><td>${t.exit}</td><td>${fmt(t.entry_px)}</td><td>${fmt(t.exit_px)}</td><td>${esc(t.reason)}</td>` +
      `<td class="${t.gross_pts > 0 ? "up" : t.gross_pts < 0 ? "down" : "flat"}">${signed(t.gross_pts)}</td><td>${fmt(t.charges_rs)}</td>` +
      `<td class="${t.net_rs > 0 ? "up" : t.net_rs < 0 ? "down" : "flat"}">${rupees(t.net_rs)}</td></tr>`).join("");
    const hold = $("runHold");
    hold.dataset.side = s.net > 0 ? "long" : s.net < 0 ? "short" : "flat";
    $("runNet").textContent = rupees(s.net);
    $("runScope").textContent = `${lastRun.index || "all six"} · ${lastRun.days.length} sessions`;
    $("runLine").textContent = `${s.trades} trades · ${s.trades ? Math.round((s.wins / s.trades) * 100) : 0}% won · PF ${s.pf === null ? "—" : fmt(s.pf)} · max DD ${plain(s.max_dd)}`;
    renderDayPicker();
  }

  function showTrade(row) {
    const t = lastRun?.trades?.[Number(row.dataset.i)];
    if (!t) return;
    for (const r of $("tradeRows").querySelectorAll('[aria-selected="true"]')) r.removeAttribute("aria-selected");
    row.setAttribute("aria-selected", "true");
    focus = t;
    if (frame !== "tick") selectFrame("tick");
    const same = t.index === index && t.day === day && replay && replay.index === index && replay.day === day;
    index = t.index; day = t.day; store.set("bt.index", index); store.set("bt.day", day);
    renderIndexPicker(); renderDayPicker(); renderPivotSummary();
    if (same) draw(); else loadDay();
    $("desk").scrollIntoView({ behavior: "smooth", block: "start" });
  }

  // ── Tune ─────────────────────────────────────────────────────────────────
  const round3 = (x) => Math.round(x * 1000) / 1000;
  function defaults(key) {
    if (key in PIVOT_POS) {
      const n = $("tuneIndex").value || index, v = S.pivots[n][PIVOT_POS[key]];
      if (!v) return key === "min_leg" ? "0, 20, 40, 60, 80, 100" : `0, ${S.pivots[n][0]}`;
      return [0.5, 0.75, 1, 1.25, 1.5, 2].map((f) => round3(v * f)).join(", ");
    }
    if (key === "max_hold_s") return "0, 300, 600, 1200, 1800";
    return (META.grid[key] || []).join(", ");
  }

  function parseValues(key, text) {
    const parts = text.split(",").map((s) => s.trim()).filter(Boolean);
    if (["exit_on", "order", "last_entry", "needle"].includes(key)) return parts;
    const out = [];
    for (const part of parts) {
      const m = part.match(/^(-?[\d.]+)\s*:\s*(-?[\d.]+)\s*:\s*([\d.]+)$/);
      if (m) {
        const [a, b, s] = [Number(m[1]), Number(m[2]), Number(m[3])];
        if (!(s > 0) || b < a || (b - a) / s > 400) throw new Error(`${LABEL[key]}: bad range ${part}`);
        for (let x = a; x <= b + s * 1e-9; x += s) out.push(round3(x));
      } else {
        const x = Number(part);
        if (!Number.isFinite(x)) throw new Error(`${LABEL[key]}: ${part} is not a number`);
        out.push(x);
      }
    }
    return [...new Set(out)];
  }

  function fillTuneKeys() {
    const all = $("tuneIndex").value === "";
    const opt = (k) => `<option value="${k}"${all && k in PIVOT_POS ? " disabled" : ""}>${LABEL[k]}</option>`;
    const k1 = $("tuneKey1").value, k2 = $("tuneKey2").value;
    $("tuneKey1").innerHTML = META.tune_keys.map(opt).join("");
    $("tuneKey2").innerHTML = `<option value="">— none —</option>${META.tune_keys.map(opt).join("")}`;
    const ok = (k) => k && !(all && k in PIVOT_POS);
    $("tuneKey1").value = ok(k1) ? k1 : (all ? "stop" : "threshold");
    $("tuneKey2").value = ok(k2) ? k2 : "";
    $("tuneVals1").value = defaults($("tuneKey1").value);
    $("tuneVals2").value = $("tuneKey2").value ? defaults($("tuneKey2").value) : "";
    $("tuneVals2").disabled = !$("tuneKey2").value;
  }

  let pollTimer = 0, lastTune = null;
  async function tune(event) {
    event.preventDefault();
    let axes, span;
    try {
      span = range();
      const k1 = $("tuneKey1").value, k2 = $("tuneKey2").value;
      if (k1 === k2) throw new Error("Setting 1 and setting 2 must differ.");
      axes = [[k1, parseValues(k1, $("tuneVals1").value)]];
      if (k2) axes.push([k2, parseValues(k2, $("tuneVals2").value)]);
      const n = axes.reduce((a, [, v]) => a * v.length, 1);
      if (!n) throw new Error("Give at least one value for each setting.");
      if (n > META.max_combos) throw new Error(`${n} combinations: at most ${META.max_combos}.`);
    } catch (e) { setStatus($("tuneStatus"), "bad", e.message); return; }
    const name = $("tuneIndex").value;
    try {
      await api("/api/tune", { settings: S, index: name, axes, ...span, train: Number($("tuneTrain").value) });
    } catch (e) { setStatus($("tuneStatus"), "bad", e.message); return; }
    lastTune = { name, axes };
    $("tuneButton").disabled = true;
    $("tuneProgress").hidden = false; $("tuneBar").style.width = "0%";
    setStatus($("tuneStatus"), "work", "Replaying every combination on every session…");
    poll();
  }

  async function poll() {
    window.clearTimeout(pollTimer);
    let job;
    try { job = await api("/api/job"); } catch (e) { pollTimer = window.setTimeout(poll, 2000); return; }
    if (job.state === "running") {
      $("tuneBar").style.width = `${Math.round(job.progress * 100)}%`;
      setStatus($("tuneStatus"), "work", `Replaying every combination on every session… ${Math.round(job.progress * 100)}%`);
      pollTimer = window.setTimeout(poll, 700);
      return;
    }
    $("tuneButton").disabled = false;
    $("tuneProgress").hidden = true;
    if (job.state === "error") { setStatus($("tuneStatus"), "bad", job.error); return; }
    if (job.state === "done") renderTune(job);
  }

  function renderTune(job) {
    const { sweep: sw, walk: wf } = job.result;
    const keys = sw.axes;
    $("tuneResults").hidden = false;
    const put = (id, v) => { $(id).textContent = rupees(v); tone($(id), v); };
    put("wfNet", wf.net); put("wfBase", wf.base);
    const best = sw.rows.reduce((a, r) => (r.net > a.net ? r : a), sw.rows[0]);
    put("isBest", best.net);
    const val = (v) => (typeof v === "number" ? fmt(v).replace(/\.00$/, "") : esc(v));
    $("isBestAt").textContent = `${keys.map((k, i) => `${LABEL[k]} ${val(best.values[i])}`).join(" · ")} · HINDSIGHT`;
    $("tuneCount").textContent = nf0.format(sw.rows.length);
    $("tuneTime").textContent = `${sw.index || "ALL SIX"} · ${sw.days.length} SESSIONS · ${job.seconds}S`;
    $("wfDays").textContent = wf.test_days ? `${wf.test_days} TEST SESSIONS · TRAIN ${wf.train ? `LAST ${wf.train}` : "ALL BEFORE"}` : "NEEDS MORE SESSIONS";
    const cls = (v) => (v > 0 ? "up" : v < 0 ? "down" : "flat");
    $("wfRows").innerHTML = wf.rows.map((r) => `<tr><td>${r.day}</td><td class="pick">${r.pick.map(val).join(" · ")}</td>` +
      `<td class="${cls(r.train_net)}">${rupees(r.train_net)}</td><td class="${cls(r.net)}">${rupees(r.net)}</td><td class="${cls(r.base)}">${rupees(r.base)}</td></tr>`).join("")
      || `<tr><td colspan="5">Walk-forward needs more sessions in range than the train window.</td></tr>`;
    $("sweepHead").innerHTML = keys.map((k) => `<th scope="col">${esc(LABEL[k]).toUpperCase()}</th>`).join("") +
      `<th scope="col">TRADES</th><th scope="col">WIN</th><th scope="col">NET ₹</th><th scope="col">1ST HALF</th><th scope="col">2ND HALF</th><th scope="col">UP DAYS</th>`;
    const order = sw.rows.map((r, i) => i).sort((a, b) => sw.rows[b].net - sw.rows[a].net);
    $("sweepRows").innerHTML = order.map((i) => {
      const r = sw.rows[i];
      return `<tr data-i="${i}" tabindex="0"${r.is_base ? " data-base" : ""}>${r.values.map((v) => `<td>${val(v)}</td>`).join("")}` +
        `<td>${r.trades}</td><td>${r.trades ? Math.round((r.wins / r.trades) * 100) : 0}%</td><td class="${cls(r.net)}">${rupees(r.net)}</td>` +
        `<td class="${cls(r.h1)}">${rupees(r.h1)}</td><td class="${cls(r.h2)}">${rupees(r.h2)}</td><td>${r.up_days}/${sw.days.length}</td></tr>`;
    }).join("");
    $("sweepRows").closest("table").classList.add("bt-click");
    lastTune.result = sw;
    setStatus($("tuneStatus"), "", `Done. Out of sample ${rupees(wf.net)} against ${rupees(wf.base)} for the settings now, on the same ${wf.test_days} session(s). Select a row to use it.`);
  }

  function useCombo(row) {
    const sw = lastTune?.result;
    const r = sw?.rows?.[Number(row.dataset.i)];
    if (!r) return;
    for (const x of $("sweepRows").querySelectorAll('[aria-selected="true"]')) x.removeAttribute("aria-selected");
    row.setAttribute("aria-selected", "true");
    sw.axes.forEach((k, i) => {
      if (k in PIVOT_POS) S.pivots[sw.index][PIVOT_POS[k]] = r.values[i];
      else S[k] = r.values[i];
    });
    saveSettings(); fillForm(); loadDay();
    toast(`Using ${sw.axes.map((k, i) => `${LABEL[k]} ${r.values[i]}`).join(", ")}${sw.index ? ` on ${sw.index}` : ""}. Press RUN to backtest it.`);
  }

  // ── Frames and windows ───────────────────────────────────────────────────
  function selectFrame(next) {
    frame = next;
    for (const b of document.querySelectorAll("#deskFrames [data-frame]")) b.setAttribute("aria-pressed", String(b.dataset.frame === frame));
    $("deskWindows").hidden = frame !== "tick";
    draw();
  }

  function selectWindow(next) {
    tickWindow = next;
    for (const b of document.querySelectorAll("#deskWindows [data-window]")) b.setAttribute("aria-pressed", String(b.dataset.window === tickWindow));
    chart.window = tickWindow === "day" ? null : Number(tickWindow) * 60e3;
    chart.resetView(); chart.schedule();
  }

  // ── Data ─────────────────────────────────────────────────────────────────
  function renderData() {
    const pod = $("dataPod");
    pod.dataset.state = META.days.length ? "live" : "error";
    $("dataLabel").textContent = META.days.length
      ? `${META.days.length} SESSIONS · ${(META.ticks / 1e6).toFixed(1)}M TICKS` : "NO TICKS · SYNC";
    pod.title = META.days.length ? `${META.days[0]} to ${META.days[META.days.length - 1]}, ${nf0.format(META.ticks)} ticks of ${INDICES.join(", ")}` : "";
  }

  async function sync() {
    const button = $("syncButton");
    button.disabled = true;
    toast("Copying finished sessions from Lune's database…");
    try {
      const r = await api("/api/sync", {}, 600000);
      META = r.meta;
      renderData(); fillRange(); renderDayPicker(); renderPivotSummary();
      toast(r.sync.added ? `${r.sync.added} new index-session${r.sync.added === 1 ? "" : "s"} copied and verified.` : "Already up to date.");
      if (!day || !META.days.includes(day)) { day = META.days[META.days.length - 1]; renderDayPicker(); loadDay(); }
    } catch (e) { toast(`Sync failed: ${e.message}`); }
    finally { button.disabled = false; }
  }

  // ── Wiring ───────────────────────────────────────────────────────────────
  async function start() {
    try { META = await api("/api/meta"); } catch (e) {
      setStatus($("ticketStatus"), "bad", e.message);
      setVeil("error", "Backtester unavailable", e.message);
      return;
    }
    let saved = null;
    try { saved = JSON.parse(store.get(SETTINGS_KEY) || "null"); } catch (_e) { saved = null; }
    if (saved && typeof saved === "object" && !Array.isArray(saved)) {
      saved = { ...META.scaled, ...saved };   // keys added since
      if (![...document.querySelector("select[name=needle]").options].some((o) => o.value === saved.needle)) saved.needle = "off";
    }
    S = validSettings(saved) ? saved : structuredClone(META.scaled);
    if (!META.days.includes(day)) day = META.days[META.days.length - 1] || null;
    renderData(); renderIndexPicker(); fillForm(); fillRange(); renderDayPicker(); fillTuneKeys();
    $("ticketTag").dataset.desk = "ready"; $("ticketTag").textContent = "Ready";
    setStatus($("ticketStatus"), "", META.days.length
      ? `${META.days.length} sessions of ticks, ${META.days[0]} to ${META.days[META.days.length - 1]}. Settings ${validSettings(saved) ? "from your last visit" : "scaled legs (PIVOTS → Scaled legs), leg filter 2× R"}.`
      : "No tick data yet: press SYNC.");
    loadDay();
  }

  document.querySelector("#indexPicker").addEventListener("click", (e) => {
    const chip = e.target.closest("[data-index]"); if (chip) selectIndex(chip.dataset.index);
  });
  $("dayPicker").addEventListener("click", (e) => { const b = e.target.closest("[data-day]"); if (b && !b.disabled) selectDay(b.dataset.day); });
  $("deskFrames").addEventListener("click", (e) => { const b = e.target.closest("[data-frame]"); if (b) selectFrame(b.dataset.frame); });
  $("deskWindows").addEventListener("click", (e) => { const b = e.target.closest("[data-window]"); if (b) selectWindow(b.dataset.window); });
  $("settingsFormMain").addEventListener("change", (e) => {
    if (!e.target.name || !S) return;
    if (readField(e.target)) {
      if (e.target.name === "needle") Object.assign(S, NEEDLE_PRESET[e.target.value] || {});
      saveSettings(); fillForm(); loadDaySoon();
      if (!$("settingsFormMain").querySelector("[data-bad]")) setStatus($("ticketStatus"), "", "Settings changed. The chart replays them; press RUN to backtest every session.");
    }
    else setStatus($("ticketStatus"), "bad", `${e.target.closest("label")?.querySelector("span")?.textContent || e.target.name}: not a valid value.`);
  });
  // a number replays as it is typed, not only on Enter or leaving the field
  $("settingsFormMain").addEventListener("input", (e) => {
    if (!S || !NUMERIC.includes(e.target.name) || e.target.value.trim() === "") return;
    if (readField(e.target)) { saveSettings(); renderPivotSummary(); loadDaySoon(); }
  });
  $("settingsFormMain").addEventListener("submit", (e) => e.preventDefault());
  $("runButton").addEventListener("click", () => run());
  $("rangeForm").addEventListener("submit", run);
  $("liveButton").addEventListener("click", () => {
    S = structuredClone(META.live); saveSettings(); fillForm(); loadDay();
    setStatus($("ticketStatus"), "", "Settings reset to what Lune trades now (as of the last SYNC).");
  });
  $("pivotsButton").addEventListener("click", openPivots);
  $("settingsOpen").addEventListener("click", openPivots);
  $("settingsClose").addEventListener("click", () => $("settings").close());
  $("settingsCancel").addEventListener("click", () => $("settings").close());
  $("settingsForm").addEventListener("submit", applyPivots);
  $("syncButton").addEventListener("click", sync);
  $("tuneForm").addEventListener("submit", tune);
  $("tuneIndex").addEventListener("change", fillTuneKeys);
  $("tuneKey1").addEventListener("change", () => { $("tuneVals1").value = defaults($("tuneKey1").value); });
  $("tuneKey2").addEventListener("change", () => {
    $("tuneVals2").disabled = !$("tuneKey2").value;
    $("tuneVals2").value = $("tuneKey2").value ? defaults($("tuneKey2").value) : "";
  });
  for (const [id, fn] of [["tradeRows", showTrade], ["sweepRows", useCombo]]) {
    $(id).addEventListener("click", (e) => { const r = e.target.closest("tr[data-i]"); if (r) fn(r); });
    $(id).addEventListener("keydown", (e) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      const r = e.target.closest("tr[data-i]"); if (r) { e.preventDefault(); fn(r); }
    });
  }
  start();
})();
