// ============================================================================
// WhatsHub Engine v2.1 - Motor Baileys com Persistência + Mídia + Validação BR
// ----------------------------------------------------------------------------
// v3 — Contrato completo de mídia no webhook:
//   - pushName, fromMe, isPtt
//   - mediaMimeType, mediaFileName, mediaSizeBytes, mediaDurationSeconds
//   - GET /media/:messageId enriquecido (Content-Disposition com filename real)
// ============================================================================

const express = require("express");
const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  downloadMediaMessage,
} = require("@whiskeysockets/baileys");
const { v4: uuidv4 } = require("uuid");
const QRCode = require("qrcode");
const pino = require("pino");
const path = require("path");
const fs = require("fs");
const os = require("os");
const axios = require("axios");

// ---------- Configuração ----------
const PORT = process.env.PORT || 3000;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || process.env.WHATSAPI_ADMIN_TOKEN;
const SESSIONS_DIR =
  process.env.SESSIONS_DIR || path.join(process.cwd(), "sessions");

if (!fs.existsSync(SESSIONS_DIR)) {
  fs.mkdirSync(SESSIONS_DIR, { recursive: true });
}

// ---------- Helpers de extração de metadata de mídia (contrato v2.1) ----------
function detectMessageType(m) {
  if (m.imageMessage) return "image";
  if (m.videoMessage) return "video";
  if (m.audioMessage) return "audio";
  if (m.documentMessage) return "document";
  if (m.documentWithCaptionMessage) return "document";
  if (m.stickerMessage) return "sticker";
  if (m.locationMessage) return "location";
  if (m.contactMessage) return "contact";
  return "text";
}

function extractMediaMeta(m, messageType) {
  if (messageType === "text" || messageType === "location" || messageType === "contact") {
    return {
      mediaMimeType: null,
      mediaFileName: null,
      mediaSizeBytes: null,
      mediaDurationSeconds: null,
      isPtt: false,
    };
  }

  const docMsg = m.documentMessage || m.documentWithCaptionMessage?.message?.documentMessage;
  const node =
    m.imageMessage ||
    m.videoMessage ||
    m.audioMessage ||
    docMsg ||
    m.stickerMessage ||
    {};

  return {
    mediaMimeType: node.mimetype || null,
    mediaFileName: docMsg?.fileName || node.fileName || null,
    mediaSizeBytes: node.fileLength ? Number(node.fileLength) : null,
    mediaDurationSeconds: node.seconds || null,
    isPtt: !!(m.audioMessage && m.audioMessage.ptt),
  };
}

// ---------- Gerenciador de Sessões ----------
class SessionManager {
  constructor() {
    this.sessions = new Map();
    this.reconnectAttempts = new Map();
  }

  getByToken(token) {
    if (!token) return null;
    for (const [, s] of this.sessions) {
      if (s.token === token) return s;
    }
    return null;
  }

  async create(opts = {}) {
    const id = opts.id || uuidv4();
    const token = opts.token || uuidv4();

    const session = {
      id,
      token,
      socket: null,
      status: "disconnected",
      qrcode: null,
      phone: null,
      webhook: null,
      messageIndex: new Map(),
      lastDisconnectReason: null,
      createdAt: new Date().toISOString(),
    };

    this.sessions.set(id, session);
    this._persistMeta(session);
    return { id, token };
  }

  _persistMeta(session) {
    const dir = path.join(SESSIONS_DIR, session.id);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, "_meta.json"),
      JSON.stringify({
        id: session.id,
        token: session.token,
        webhook: session.webhook,
        createdAt: session.createdAt,
      }, null, 2)
    );
  }

  async recoverPersistedSessions() {
    if (!fs.existsSync(SESSIONS_DIR)) return;
    const dirs = fs.readdirSync(SESSIONS_DIR).filter((d) =>
      fs.statSync(path.join(SESSIONS_DIR, d)).isDirectory()
    );

    console.log(`♻️  Recuperando ${dirs.length} sessão(ões) persistida(s)...`);

    for (const id of dirs) {
      const metaPath = path.join(SESSIONS_DIR, id, "_meta.json");
      const credsPath = path.join(SESSIONS_DIR, id, "creds.json");

      if (!fs.existsSync(metaPath)) continue;

      try {
        const meta = JSON.parse(fs.readFileSync(metaPath, "utf-8"));
        const session = {
          id: meta.id,
          token: meta.token,
          socket: null,
          status: "disconnected",
          qrcode: null,
          phone: null,
          webhook: meta.webhook || null,
          messageIndex: new Map(),
          lastDisconnectReason: null,
          createdAt: meta.createdAt,
        };
        this.sessions.set(id, session);

        if (fs.existsSync(credsPath)) {
          console.log(`   → Reconectando ${id}...`);
          this.connect(id).catch((err) =>
            console.log(`   ⚠️ Falha ao reconectar ${id}: ${err.message}`)
          );
        } else {
          console.log(`   → ${id} sem credenciais, aguardando QR`);
        }
      } catch (err) {
        console.log(`   ⚠️ Erro ao recuperar ${id}: ${err.message}`);
      }
    }
  }

  _decideReconnect(code) {
    const manual = [
      DisconnectReason.loggedOut,
      DisconnectReason.connectionReplaced,
      DisconnectReason.badSession,
      DisconnectReason.multideviceMismatch,
    ];
    if (manual.includes(code)) return false;
    return true;
  }

  async connect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const sessionDir = path.join(SESSIONS_DIR, id);
    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
    const { version } = await fetchLatestBaileysVersion();

    const socket = makeWASocket({
      version,
      logger: pino({ level: "silent" }),
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, pino({ level: "silent" })),
      },
      browser: ["Ubuntu", "Chrome", "20.0.04"],
      markOnlineOnConnect: true,
      printQRInTerminal: false,
      keepAliveIntervalMs: 30000,
      shouldSyncHistoryMessage: () => false,
      syncFullHistory: false,
    });

    session.socket = socket;
    session.status = "connecting";

    socket.ev.on("creds.update", saveCreds);

    // ------- Mensagens recebidas/enviadas + Webhook (contrato v2.1 COMPLETO) -------
    socket.ev.on("messages.upsert", async ({ messages }) => {
      for (const msg of messages) {
        if (!msg.message) continue;

        const jid = msg.key.remoteJid;
        if (!jid || jid === "status@broadcast") continue;

        const messageId = msg.key.id;

        const text =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message.imageMessage?.caption ||
          msg.message.videoMessage?.caption ||
          msg.message.documentMessage?.caption ||
          msg.message.documentWithCaptionMessage?.message?.documentMessage?.caption ||
          "";

        const m = msg.message;
        const messageType = detectMessageType(m);
        const isMedia = messageType !== "text" && messageType !== "location" && messageType !== "contact";
        const mediaUrl = isMedia ? `/media/${messageId}` : null;
        const meta = extractMediaMeta(m, messageType);

        session.messageIndex.set(messageId, {
          key: msg.key,
          message: msg.message,
        });

        if (session.messageIndex.size > 1000) {
          const firstKey = session.messageIndex.keys().next().value;
          session.messageIndex.delete(firstKey);
        }

        console.log(
          `📩 [${id}] ${msg.key.fromMe ? "→" : "←"} ${jid} (${messageType}${meta.isPtt ? "/ptt" : ""}): ${text.slice(0, 50)}`
        );

        if (session.webhook) {
          try {
            await axios.post(session.webhook, {
              event: "message",
              instanceId: session.id,
              from: jid,
              fromMe: msg.key.fromMe || false,
              messageId,
              pushName: msg.pushName || null,
              timestamp: msg.messageTimestamp || Math.floor(Date.now() / 1000),
              messageType,
              text,
              message: text,
              mediaUrl,
              mediaMimeType: meta.mediaMimeType,
              mediaFileName: meta.mediaFileName,
              mediaSizeBytes: meta.mediaSizeBytes,
              mediaDurationSeconds: meta.mediaDurationSeconds,
              isPtt: meta.isPtt,
            }, { timeout: 10000 });
          } catch (err) {
            console.log(`⚠️ [${id}] Falha webhook: ${err.message}`);
          }
        }
      }
    });

    socket.ev.on("connection.update", async (update) => {
      const { connection, qr, lastDisconnect } = update;

      if (qr) {
        session.qrcode = await QRCode.toDataURL(qr);
        console.log(`📷 [${id}] QR Code gerado (expira em 60s)`);
        if (session.qrTimeout) clearTimeout(session.qrTimeout);
        session.qrTimeout = setTimeout(() => {
          if (session.status !== "connected") {
            session.qrcode = null;
            console.log(`⌛ [${id}] QR Code expirado (60s sem leitura)`);
          }
        }, 60_000);
      }

      if (connection === "open") {
        session.status = "connected";
        session.qrcode = null;
        if (session.qrTimeout) { clearTimeout(session.qrTimeout); session.qrTimeout = null; }
        session.lastDisconnectReason = null;
        this.reconnectAttempts.set(id, 0);

        const jid = socket.user?.id;
        if (jid) session.phone = jid.split("@")[0].split(":")[0];

        console.log(`✅ [${id}] Conectado: ${session.phone}`);
      }

      if (connection === "close") {
        const code = lastDisconnect?.error?.output?.statusCode;
        const reason = lastDisconnect?.error?.message || "unknown";
        session.lastDisconnectReason = `${code || "?"}: ${reason}`;
        session.socket = null;

        console.log(`🔌 [${id}] Desconectado (${code}): ${reason}`);

        const shouldReconnect = this._decideReconnect(code);

        if (shouldReconnect) {
          const attempts = (this.reconnectAttempts.get(id) || 0) + 1;
          this.reconnectAttempts.set(id, attempts);
          const delay = Math.min(3000 * Math.pow(2, attempts - 1), 60000);

          console.log(`🔄 [${id}] Reconectando em ${delay / 1000}s (tentativa ${attempts})`);
          session.status = "connecting";
          setTimeout(() => {
            this.connect(id).catch((err) =>
              console.log(`⚠️ [${id}] Erro na reconexão: ${err.message}`)
            );
          }, delay);
        } else {
          session.status = "disconnected";
          console.log(`❌ [${id}] Reconexão automática desativada (ação manual necessária)`);
        }
      }
    });

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Timeout aguardando QR Code (30s)"));
      }, 30000);

      const timer = setInterval(() => {
        if (session.qrcode) {
          clearInterval(timer);
          clearTimeout(timeout);
          resolve(session.qrcode);
        }
        if (session.status === "connected") {
          clearInterval(timer);
          clearTimeout(timeout);
          resolve(null);
        }
      }, 500);
    });
  }

  async disconnect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    if (session.socket) {
      try { await session.socket.logout(); }
      catch (_) {
        try { session.socket.end(); } catch (_) {}
      }
    }

    session.socket = null;
    session.status = "disconnected";
    session.qrcode = null;
    session.phone = null;
    this.reconnectAttempts.set(id, 0);

    const sessionDir = path.join(SESSIONS_DIR, id);
    if (fs.existsSync(sessionDir)) {
      const files = fs.readdirSync(sessionDir);
      for (const f of files) {
        if (f !== "_meta.json") {
          fs.rmSync(path.join(sessionDir, f), { recursive: true, force: true });
        }
      }
    }

    console.log(`🔌 [${id}] Desconectado manualmente`);
  }

  getStatus(id) {
    const session = this.sessions.get(id);
    if (!session) return { status: "not_found" };
    return {
      status: session.status,
      phone: session.phone,
      qrcode: session.qrcode,
      lastDisconnectReason: session.lastDisconnectReason,
    };
  }

  async sendMessage(id, { phone, message }) {
    const session = this.sessions.get(id);
    if (!session?.socket) throw new Error("Instance not connected");

    const clean = String(phone).replace(/\D/g, "");
    if (!clean || clean.length < 10) {
      throw new Error("Número inválido: " + phone);
    }

    let targetJid;
    try {
      const [result] = await session.socket.onWhatsApp(clean);

      if (!result || !result.exists) {
        const alt = tryBrazilianAlternative(clean);
        if (alt) {
          const [altResult] = await session.socket.onWhatsApp(alt);
          if (!altResult || !altResult.exists) {
            throw new Error(`Número ${clean} não encontrado (tentou ${alt} também)`);
          }
          targetJid = altResult.jid;
          console.log(`📱 Corrigido: ${clean} → ${alt} (JID: ${targetJid})`);
        } else {
          throw new Error(`Número ${clean} não encontrado no WhatsApp`);
        }
      } else {
        targetJid = result.jid;
      }
    } catch (err) {
      if (err.message.includes("não encontrado")) throw err;
      console.log(`⚠️ onWhatsApp falhou, tentando direto: ${err.message}`);
      targetJid = `${clean}@s.whatsapp.net`;
    }

    const sent = await session.socket.sendMessage(targetJid, { text: message });
    const delivered = !!(sent?.key?.id);

    if (sent?.key?.id) {
      session.messageIndex.set(sent.key.id, {
        key: sent.key,
        message: sent.message,
      });
    }

    return {
      success: delivered,
      delivered,
      messageId: sent?.key?.id || null,
      to: targetJid,
    };
  }

  setWebhook(id, url) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    session.webhook = url;
    this._persistMeta(session);
    return { webhook: url };
  }

  // Baixa mídia + retorna metadata (mimetype + filename real)
  async downloadMedia(id, messageId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const rawMsg = session.messageIndex.get(messageId);
    if (!rawMsg) return { found: false };

    try {
      const buffer = await downloadMediaMessage(rawMsg, "buffer", {});
      const m = rawMsg.message || {};
      const docMsg = m.documentMessage || m.documentWithCaptionMessage?.message?.documentMessage;
      const node =
        m.imageMessage ||
        m.videoMessage ||
        m.audioMessage ||
        docMsg ||
        m.stickerMessage ||
        {};

      return {
        found: true,
        buffer,
        mimetype: node.mimetype || "application/octet-stream",
        filename: docMsg?.fileName || node.fileName || null,
      };
    } catch (err) {
      console.log(`⚠️ [${id}] Erro ao baixar mídia: ${err.message}`);
      return { found: false };
    }
  }

  listAll() {
    return Array.from(this.sessions.values()).map((s) => ({
      id: s.id,
      status: s.status,
      phone: s.phone,
      webhook: s.webhook,
      lastDisconnectReason: s.lastDisconnectReason,
      createdAt: s.createdAt,
    }));
  }
}

function tryBrazilianAlternative(phone) {
  if (!phone.startsWith("55")) return null;
  const ddd = phone.substring(2, 4);
  const rest = phone.substring(4);
  if (rest.length === 9 && rest.startsWith("9")) return "55" + ddd + rest.substring(1);
  if (rest.length === 8) return "55" + ddd + "9" + rest;
  return null;
}

// ============================================================================
// API HTTP
// ============================================================================
const sessions = new SessionManager();
const app = express();
app.use(express.json({ limit: "50mb" }));

function requireAdmin(req, res, next) {
  const token = req.headers["x-admin-token"];
  if (!ADMIN_TOKEN || token !== ADMIN_TOKEN) {
    return res.status(401).json({ success: false, error: "Invalid admin token" });
  }
  next();
}

function requireInstance(req, res, next) {
  const token = req.headers["x-instance-token"];
  const session = sessions.getByToken(token);
  if (!session) {
    return res.status(401).json({ success: false, error: "Invalid instance token" });
  }
  req.session = session;
  next();
}

app.get("/health", (_, res) => res.json({ ok: true, version: "2.1" }));

app.get("/system/metrics", (_, res) => {
  const mem = process.memoryUsage();
  res.json({
    uptime: process.uptime(),
    memory: {
      rss: Math.round(mem.rss / 1024 / 1024),
      heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
    },
    cpu: os.loadavg(),
    sessions: sessions.sessions.size,
    connectedSessions: Array.from(sessions.sessions.values())
      .filter((s) => s.status === "connected").length,
  });
});

app.post("/instance/create", requireAdmin, async (req, res) => {
  try {
    const result = await sessions.create();
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/instance/list", requireAdmin, (_, res) => {
  res.json({ success: true, instances: sessions.listAll() });
});

app.post("/connect", requireInstance, async (req, res) => {
  try {
    const qr = await sessions.connect(req.session.id);
    res.json({ success: true, qrcode: qr, status: req.session.status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/disconnect", requireInstance, async (req, res) => {
  try {
    await sessions.disconnect(req.session.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/status", requireInstance, (req, res) => {
  res.json({ success: true, ...sessions.getStatus(req.session.id) });
});

app.post("/send", requireInstance, async (req, res) => {
  try {
    const result = await sessions.sendMessage(req.session.id, req.body);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.post("/webhook", requireInstance, (req, res) => {
  try {
    const { url } = req.body;
    const result = sessions.setWebhook(req.session.id, url);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// Download de mídia pelo messageId (autenticado por x-instance-token)
// Retorna o binário com Content-Type correto e Content-Disposition com filename real
app.get("/media/:messageId", requireInstance, async (req, res) => {
  try {
    const result = await sessions.downloadMedia(
      req.session.id,
      req.params.messageId
    );
    if (!result.found) {
      return res.status(410).json({
        success: false,
        error: "Mensagem não está mais na memória (expirada)",
      });
    }
    res.set("Content-Type", result.mimetype);
    if (result.filename) {
      const safe = String(result.filename).replace(/["\r\n]/g, "");
      res.set("Content-Disposition", `inline; filename="${safe}"`);
    } else {
      res.set("Content-Disposition", "inline");
    }
    res.set("Cache-Control", "public, max-age=3600");
    res.send(result.buffer);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.listen(PORT, async () => {
  console.log(`🚀 WhatsHub Engine v2.1 online na porta ${PORT}`);
  console.log(`📁 Sessões em: ${SESSIONS_DIR}`);
  await sessions.recoverPersistedSessions();
});
