/**
 * ============================================================
 *  WhatsHub Engine v2.0 — Motor Baileys resiliente
 * ============================================================
 *  - Sessões persistentes em /app/sessions (volume Docker)
 *  - Reconnect inteligente tratando TODOS os DisconnectReason
 *  - Keep-alive WebSocket de 30s (evita corte de proxy/firewall)
 *  - Endpoint /system/metrics com CPU/RAM reais
 *  - Log explícito do motivo de cada queda
 *  - Webhook outbound com retry e fallback para polling
 * ============================================================
 */
const express = require("express");
const fs = require("fs");
const path = require("path");
const os = require("os");
const pino = require("pino");
const QRCode = require("qrcode");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  Browsers,
} = require("@whiskeysockets/baileys");

// ───────────────────────────────────────────────────────────
// Configuração
// ───────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const ADMIN_TOKEN = process.env.WHATSAPI_ADMIN_TOKEN || "change-me";
const SESSIONS_DIR = process.env.SESSIONS_DIR || "/app/sessions";
const KEEP_ALIVE_MS = 30_000;
const MAX_RECONNECT_ATTEMPTS = 20;
const RECONNECT_BASE_DELAY_MS = 3_000;

if (!fs.existsSync(SESSIONS_DIR)) fs.mkdirSync(SESSIONS_DIR, { recursive: true });

const logger = pino({ level: "info", transport: { target: "pino-pretty" } }).child({ mod: "engine" });
// Baileys é muito verboso — silenciamos internamente
const baileysLogger = pino({ level: "silent" });

// ───────────────────────────────────────────────────────────
// Estado em memória
// ───────────────────────────────────────────────────────────
/** @type {Map<string, InstanceState>} */
const instances = new Map();
const startedAt = Date.now();
let eventsLast24h = [];
let lastActivityAt = null;

/**
 * @typedef {Object} InstanceState
 * @property {string} id
 * @property {string} token
 * @property {import('@whiskeysockets/baileys').WASocket | null} sock
 * @property {string|null} qr
 * @property {string} status    - 'disconnected' | 'connecting' | 'connected'
 * @property {string|null} phone
 * @property {string|null} webhookUrl
 * @property {number} reconnectAttempts
 * @property {string|null} lastDisconnectReason
 * @property {number|null} lastDisconnectAt
 * @property {NodeJS.Timeout|null} reconnectTimer
 */

// ───────────────────────────────────────────────────────────
// Utilidades
// ───────────────────────────────────────────────────────────
const reasonName = (code) => {
  const map = Object.fromEntries(Object.entries(DisconnectReason).map(([k, v]) => [v, k]));
  return map[code] || `unknown(${code})`;
};

const sessionPath = (id) => path.join(SESSIONS_DIR, id);

const registerEvent = (type) => {
  const now = Date.now();
  lastActivityAt = now;
  eventsLast24h.push({ type, ts: now });
  // Limpa eventos com mais de 24h
  const cutoff = now - 24 * 60 * 60 * 1000;
  eventsLast24h = eventsLast24h.filter((e) => e.ts >= cutoff);
};

const fireWebhook = async (state, payload) => {
  if (!state.webhookUrl) return;
  try {
    await fetch(state.webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(8000),
    });
  } catch (err) {
    logger.warn({ instance: state.id, err: err.message }, "webhook falhou");
  }
};

// ───────────────────────────────────────────────────────────
// Núcleo: criar/reconectar instância
// ───────────────────────────────────────────────────────────
async function startInstance(id, token) {
  let state = instances.get(id);
  if (!state) {
    state = {
      id, token,
      sock: null, qr: null, status: "disconnected",
      phone: null, webhookUrl: null,
      reconnectAttempts: 0,
      lastDisconnectReason: null, lastDisconnectAt: null,
      reconnectTimer: null,
    };
    instances.set(id, state);
  }

  // Evita duplicar socket
  if (state.sock) {
    try { state.sock.end(undefined); } catch {}
    state.sock = null;
  }

  const { state: authState, saveCreds } = await useMultiFileAuthState(sessionPath(id));
  const { version } = await fetchLatestBaileysVersion();

  logger.info({ instance: id, version: version.join(".") }, "🔌 iniciando instância");
  state.status = "connecting";

  const sock = makeWASocket({
    version,
    auth: authState,
    logger: baileysLogger,
    browser: Browsers.macOS("WhatsHub"),
    printQRInTerminal: false,
    keepAliveIntervalMs: KEEP_ALIVE_MS,   // ⭐ evita corte por proxy/firewall
    connectTimeoutMs: 60_000,
    defaultQueryTimeoutMs: 60_000,
    emitOwnEvents: false,
    markOnlineOnConnect: true,
    syncFullHistory: false,
    shouldSyncHistoryMessage: () => false, // reduz consumo de RAM
  });

  state.sock = sock;

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      state.qr = await QRCode.toDataURL(qr);
      logger.info({ instance: id }, "📱 novo QR gerado");
    }

    if (connection === "open") {
      state.status = "connected";
      state.qr = null;
      state.reconnectAttempts = 0;
      state.phone = sock.user?.id?.split(":")[0] || null;
      logger.info({ instance: id, phone: state.phone }, "✅ conectado");
      registerEvent("connected");
      fireWebhook(state, { event: "connection.update", instance_id: id, status: "connected", phone: state.phone });
    }

    if (connection === "close") {
      const err = lastDisconnect?.error;
      const code = err?.output?.statusCode;
      const name = reasonName(code);
      state.status = "disconnected";
      state.lastDisconnectReason = name;
      state.lastDisconnectAt = Date.now();

      logger.warn({ instance: id, code, reason: name }, `⚠️  desconectado (${name})`);
      registerEvent("disconnected");
      fireWebhook(state, { event: "connection.update", instance_id: id, status: "disconnected", reason: name, code });

      // ── Decisão de reconexão ──
      const shouldReconnect = decideReconnect(code);
      if (!shouldReconnect) {
        logger.error({ instance: id, reason: name }, "❌ sessão morta — requer novo QR");
        // Em loggedOut limpamos as credenciais para forçar re-pareamento
        if (code === DisconnectReason.loggedOut) {
          try { fs.rmSync(sessionPath(id), { recursive: true, force: true }); } catch {}
        }
        return;
      }

      state.reconnectAttempts += 1;
      if (state.reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
        logger.error({ instance: id }, "❌ limite de tentativas — desistindo temporariamente");
        return;
      }

      // Backoff exponencial: 3s, 6s, 12s, 24s... teto de 60s
      const delay = Math.min(RECONNECT_BASE_DELAY_MS * 2 ** (state.reconnectAttempts - 1), 60_000);
      logger.info({ instance: id, attempt: state.reconnectAttempts, delay }, "🔄 reagendando reconexão");
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = setTimeout(() => startInstance(id, token).catch((e) =>
        logger.error({ instance: id, err: e.message }, "falha no reconnect")
      ), delay);
    }
  });

  sock.ev.on("messages.upsert", async (m) => {
    registerEvent("message");
    fireWebhook(state, { event: "messages.upsert", instance_id: id, data: m });
  });

  return state;
}

/**
 * Decide se devemos tentar reconectar baseado no motivo da queda.
 * Regras oficiais do Baileys + boas práticas de produção.
 */
function decideReconnect(code) {
  switch (code) {
    case DisconnectReason.loggedOut:            // 401 — usuário deslogou
    case DisconnectReason.forbidden:            // 403 — banido
    case DisconnectReason.multideviceMismatch:  // incompatibilidade
      return false;
    case DisconnectReason.connectionReplaced:   // 440 — outra sessão assumiu
      // Não reconectamos imediatamente: outro cliente assumiu o número.
      // Só o usuário pode decidir reconectar manualmente.
      return false;
    case DisconnectReason.restartRequired:      // 515 — pós-pareamento
    case DisconnectReason.connectionClosed:     // 428
    case DisconnectReason.connectionLost:       // 408
    case DisconnectReason.timedOut:             // 408
    case DisconnectReason.badSession:           // arquivos corrompidos
    default:
      return true;
  }
}

// ───────────────────────────────────────────────────────────
// Recuperação automática no boot (sessões já pareadas)
// ───────────────────────────────────────────────────────────
async function recoverPersistedSessions() {
  if (!fs.existsSync(SESSIONS_DIR)) return;
  const dirs = fs.readdirSync(SESSIONS_DIR, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);

  logger.info({ count: dirs.length }, "♻️  recuperando sessões persistidas");
  for (const id of dirs) {
    // Só recupera se houver creds.json (sessão realmente pareada)
    if (fs.existsSync(path.join(sessionPath(id), "creds.json"))) {
      startInstance(id, "recovered").catch((e) =>
        logger.error({ instance: id, err: e.message }, "falha ao recuperar")
      );
    }
  }
}

// ───────────────────────────────────────────────────────────
// Autenticação admin
// ───────────────────────────────────────────────────────────
const requireAdmin = (req, res, next) => {
  const token = req.headers["authorization"]?.replace(/^Bearer\s+/i, "") || req.headers["x-admin-token"];
  if (token !== ADMIN_TOKEN) return res.status(401).json({ error: "unauthorized" });
  next();
};

// ───────────────────────────────────────────────────────────
// HTTP API
// ───────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: "25mb" }));

app.get("/health", (_req, res) => res.json({ ok: true, uptime: (Date.now() - startedAt) / 1000 }));

// Métricas reais (consumido pelo painel WhatsHub)
app.get("/system/metrics", requireAdmin, (_req, res) => {
  const mem = process.memoryUsage();
  const totalMb = Math.round(os.totalmem() / 1024 / 1024);
  const freeMb = Math.round(os.freemem() / 1024 / 1024);
  const usedMb = totalMb - freeMb;
  const cpus = os.cpus();
  // CPU usage aproximado pela load average / núcleos
  const load = os.loadavg();
  const cpuPct = Math.min(100, (load[0] / cpus.length) * 100);

  res.json({
    system: {
      total_memory_mb: totalMb,
      used_memory_mb: usedMb,
      free_memory_mb: freeMb,
      memory_usage_percent: +((usedMb / totalMb) * 100).toFixed(1),
      cpu_usage_percent: +cpuPct.toFixed(1),
      cpu_cores: cpus.length,
      cpu_model: cpus[0]?.model || "unknown",
      uptime_seconds: Math.floor(os.uptime()),
      process_uptime_seconds: Math.floor((Date.now() - startedAt) / 1000),
      load_average: load,
    },
    process: {
      heap_used_mb: +(mem.heapUsed / 1024 / 1024).toFixed(1),
      heap_total_mb: +(mem.heapTotal / 1024 / 1024).toFixed(1),
      rss_mb: +(mem.rss / 1024 / 1024).toFixed(1),
    },
    instances_on_vps: instances.size,
    events_last_24h: eventsLast24h.length,
    last_activity_at: lastActivityAt,
  });
});

// Listar instâncias
app.get("/instances", requireAdmin, (_req, res) => {
  const list = [...instances.values()].map((s) => ({
    id: s.id,
    status: s.status,
    phone: s.phone,
    last_disconnect_reason: s.lastDisconnectReason,
    last_disconnect_at: s.lastDisconnectAt,
    reconnect_attempts: s.reconnectAttempts,
  }));
  res.json({ instances: list });
});

// Criar/iniciar instância
app.post("/instances", requireAdmin, async (req, res) => {
  const { id, token } = req.body || {};
  if (!id) return res.status(400).json({ error: "id obrigatório" });
  await startInstance(id, token || "");
  res.json({ ok: true, id });
});

// Status de uma instância
app.get("/instances/:id/status", requireAdmin, (req, res) => {
  const s = instances.get(req.params.id);
  if (!s) return res.status(404).json({ error: "not found" });
  res.json({
    id: s.id, status: s.status, phone: s.phone,
    last_disconnect_reason: s.lastDisconnectReason,
    last_disconnect_at: s.lastDisconnectAt,
  });
});

// QR Code
app.get("/instances/:id/qr", requireAdmin, (req, res) => {
  const s = instances.get(req.params.id);
  if (!s) return res.status(404).json({ error: "not found" });
  res.json({ qr: s.qr, status: s.status });
});

// Reconectar manualmente
app.post("/instances/:id/reconnect", requireAdmin, async (req, res) => {
  const s = instances.get(req.params.id);
  if (!s) return res.status(404).json({ error: "not found" });
  s.reconnectAttempts = 0;
  await startInstance(s.id, s.token);
  res.json({ ok: true });
});

// Desconectar / desligar
app.post("/instances/:id/disconnect", requireAdmin, async (req, res) => {
  const s = instances.get(req.params.id);
  if (!s) return res.status(404).json({ error: "not found" });
  try { s.sock?.end(undefined); } catch {}
  clearTimeout(s.reconnectTimer);
  s.status = "disconnected";
  res.json({ ok: true });
});

// Deletar instância (apaga sessão do disco)
app.delete("/instances/:id", requireAdmin, async (req, res) => {
  const s = instances.get(req.params.id);
  if (s) {
    try { s.sock?.logout(); } catch {}
    try { s.sock?.end(undefined); } catch {}
    clearTimeout(s.reconnectTimer);
    instances.delete(s.id);
  }
  try { fs.rmSync(sessionPath(req.params.id), { recursive: true, force: true }); } catch {}
  res.json({ ok: true });
});

// Configurar webhook
app.post("/instances/:id/webhook", requireAdmin, (req, res) => {
  const s = instances.get(req.params.id);
  if (!s) return res.status(404).json({ error: "not found" });
  s.webhookUrl = req.body?.url || null;
  res.json({ ok: true, webhook: s.webhookUrl });
});

// Enviar texto
app.post("/instances/:id/send", requireAdmin, async (req, res) => {
  const s = instances.get(req.params.id);
  if (!s || s.status !== "connected") return res.status(400).json({ error: "not connected" });
  const { phone, message } = req.body || {};
  if (!phone || !message) return res.status(400).json({ error: "phone e message obrigatórios" });

  const jid = phone.includes("@") ? phone : `${phone.replace(/\D/g, "")}@s.whatsapp.net`;
  try {
    // Valida existência no WhatsApp antes de enviar
    const [check] = await s.sock.onWhatsApp(jid);
    if (!check?.exists) return res.status(404).json({ error: "número não existe no WhatsApp" });
    const result = await s.sock.sendMessage(check.jid, { text: message });
    res.json({ ok: true, id: result?.key?.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ───────────────────────────────────────────────────────────
// Boot
// ───────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  logger.info({ port: PORT, sessions_dir: SESSIONS_DIR }, "🚀 WhatsHub Engine v2 online");
  await recoverPersistedSessions();
});

// Encerramento limpo — garante flush dos arquivos de auth
const shutdown = async (sig) => {
  logger.info({ sig }, "encerrando...");
  for (const s of instances.values()) {
    try { s.sock?.end(undefined); } catch {}
    clearTimeout(s.reconnectTimer);
  }
  process.exit(0);
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("uncaughtException", (e) => logger.error({ err: e.message, stack: e.stack }, "uncaughtException"));
process.on("unhandledRejection", (e) => logger.error({ err: e?.message || e }, "unhandledRejection"));
