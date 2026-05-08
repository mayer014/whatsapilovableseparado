// ============================================================================
// WhatsHub Engine v2.4 - Motor Baileys com Persistência + Mídia + Validação BR
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
// URL pública do motor — usada para montar mediaUrl absoluto no webhook
// Ex.: https://motor.seudominio.com (sem barra no final)
const PUBLIC_URL = (process.env.PUBLIC_URL || "").replace(/\/$/, "");

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
    this.reconnectTimers = new Map();
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
      qrRetries: 0,
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
          qrRetries: 0,
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

  _decideReconnect(code, reason = "") {
    const manual = [
      DisconnectReason.loggedOut,
      DisconnectReason.connectionReplaced,
      DisconnectReason.badSession,
      DisconnectReason.multideviceMismatch,
      DisconnectReason.timedOut,
    ];
    const text = String(reason || "").toLowerCase();
    if (manual.includes(code)) return false;
    if (text.includes("qr refs attempts ended") || text.includes("qr") || code === 408) return false;
    return true;
  }

  _clearReconnectTimer(id) {
    const timer = this.reconnectTimers.get(id);
    if (timer) clearTimeout(timer);
    this.reconnectTimers.delete(id);
  }

  async connect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    this._clearReconnectTimer(id);

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
        // mediaUrl absoluto + token de instância em query (consumer baixa direto do motor)
        const mediaUrl = isMedia
          ? (PUBLIC_URL
              ? `${PUBLIC_URL}/media/${messageId}?t=${encodeURIComponent(session.token)}`
              : `/media/${messageId}`)
          : null;
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

        // ── Resolução de número real (LID → PN) ──
        // Em chats modernos, msg.key.remoteJid pode ser "<id>@lid" (identificador interno).
        // Baileys expõe o telefone real em senderPn/participantPn (PN = Phone Number JID).
        // Enviamos todos os campos para o relay reconstruir o "from" verdadeiro.
        const senderPn = msg.key.senderPn || null;
        const participantPn = msg.key.participantPn || null;
        const remoteJidAlt = (jid && jid.endsWith("@lid"))
          ? (senderPn || participantPn || null)
          : null;
        const realFrom = senderPn || participantPn || jid;

        if (session.webhook) {
          try {
            await axios.post(session.webhook, {
              event: "message",
              instanceId: session.id,
              from: realFrom,
              rawJid: jid,
              senderPn,
              participantPn,
              remoteJidAlt,
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
            session.status = "disconnected";
            session.lastDisconnectReason = "QR Code expirado sem leitura";
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
        session.qrRetries = 0;
        this._clearReconnectTimer(id);

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

        const shouldReconnect = this._decideReconnect(code, reason);

        if (shouldReconnect) {
          const attempts = (this.reconnectAttempts.get(id) || 0) + 1;
          this.reconnectAttempts.set(id, attempts);
          const delay = Math.min(3000 * Math.pow(2, attempts - 1), 60000);
          console.log(`🔄 [${id}] Tentativa #${attempts} em ${delay}ms...`);
          const timer = setTimeout(() => {
            this.reconnectTimers.delete(id);
            this.connect(id).catch((err) =>
              console.log(`⚠️ [${id}] Reconexão falhou: ${err.message}`)
            );
          }, delay);
          this.reconnectTimers.set(id, timer);
        } else {
          console.log(`⛔ [${id}] Reconexão automática desabilitada (${code})`);
        }
      }
    });

    return { id };
  }

  async disconnect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    if (session.socket) {
      try { await session.socket.logout(); } catch {}
      session.socket = null;
    }
    session.status = "disconnected";
    return { id };
  }

  status(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    return {
      id: session.id,
      status: session.status,
      qrcode: session.qrcode,
      phone: session.phone,
      lastDisconnectReason: session.lastDisconnectReason,
    };
  }

  async send(id, recipientRaw, message) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    if (session.status !== "connected" || !session.socket) {
      throw new Error("Instance not connected");
    }

    const raw = String(recipientRaw || "").trim();
    if (!raw) throw new Error("Destinatário vazio");

    // ----- Detecção de grupo (@g.us) ou JID já formatado -----
    // Aceita: "5511999...-1700000000@g.us", "120363xxxxxxxxx@g.us",
    //         "5511999999999@s.whatsapp.net" ou número puro "5511999999999".
    const isGroupJid = raw.endsWith("@g.us");
    const isUserJid = raw.endsWith("@s.whatsapp.net");

    if (isGroupJid) {
      // Envio para grupo: usa o JID exatamente como recebido (sem normalização BR)
      try {
        const sent = await session.socket.sendMessage(raw, { text: String(message) });
        const delivered = !!sent?.key?.id;
        if (!delivered) {
          throw new Error("Envio para grupo não retornou messageId");
        }
        console.log(`📤 [${id}] → ${raw} (group): ${String(message).slice(0, 50)}`);
        return { success: true, delivered: true, messageId: sent.key.id, to: raw, isGroup: true };
      } catch (err) {
        console.log(`⚠️ [${id}] Falha envio grupo ${raw}: ${err.message}`);
        throw new Error(`Falha ao enviar para grupo: ${err.message}`);
      }
    }

    // ----- Envio 1:1 -----
    let targetJid;
    let phone;

    if (isUserJid) {
      // JID já formatado: extrai dígitos para tentativa BR alternativa
      targetJid = raw;
      phone = raw.split("@")[0].split(":")[0].replace(/\D/g, "");
    } else {
      phone = raw.replace(/\D/g, "");
      if (!phone.startsWith("55") || phone.length < 12 || phone.length > 13) {
        throw new Error(
          `Número inválido: "${recipientRaw}". Use formato 55 + DDD + número (ex: 5567999999999) ou JID @g.us para grupos`
        );
      }
      targetJid = `${phone}@s.whatsapp.net`;
    }

    let sent = null;
    let delivered = false;

    try {
      sent = await session.socket.sendMessage(targetJid, { text: String(message) });
      delivered = !!sent?.key?.id;
    } catch (err) {
      console.log(`⚠️ [${id}] Envio inicial falhou: ${err.message}`);
    }

    if (!delivered && phone) {
      const alt = tryBrazilianAlternative(phone);
      if (alt) {
        try {
          const altJid = `${alt}@s.whatsapp.net`;
          console.log(`🔁 [${id}] Tentando alternativa BR: ${alt}`);
          sent = await session.socket.sendMessage(altJid, { text: String(message) });
          delivered = !!sent?.key?.id;
        } catch (err) {
          console.log(`⚠️ [${id}] Alternativa falhou: ${err.message}`);
        }
      }
    }

    if (!delivered) {
      throw new Error("Envio não confirmado pela rede WhatsApp (sem messageId)");
    }

    return {
      success: true,
      delivered: true,
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

  // Lista grupos da sessão (Baileys groupFetchAllParticipating).
  // Retorna formato consumido pela bridge: id (@g.us), subject, participants_count, isGroup, isAdmin, announce.
  // IMPORTANTE: o JID da sessão (sock.user.id) vem com sufixo de device (ex.: "5567xxx:42@s.whatsapp.net")
  // e/ou em formato LID (xxx@lid). Os participants[] do grupo podem usar PN ou LID. Por isso normalizamos
  // para apenas a parte numérica antes de comparar — caso contrário isAdmin sempre cai em false.
  async listChats(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    if (session.status !== "connected" || !session.socket) {
      throw new Error("Instance not connected");
    }
    const sock = session.socket;

    // Extrai apenas dígitos de um JID (remove device suffix ":NN", domínio e sufixos LID)
    const numOnly = (jid) => {
      if (!jid || typeof jid !== "string") return "";
      return jid.split("@")[0].split(":")[0].replace(/\D/g, "");
    };

    const meCandidates = new Set();
    if (sock.user?.id) meCandidates.add(numOnly(sock.user.id));
    if (sock.user?.lid) meCandidates.add(numOnly(sock.user.lid));
    // Alguns builds expõem só o phone
    if (session.phone) meCandidates.add(numOnly(session.phone));

    const groupsMap = await sock.groupFetchAllParticipating();
    return Object.values(groupsMap).map((g) => {
      const participants = g.participants || [];
      // Procura o próprio número entre os participantes comparando por dígitos
      const meEntry = participants.find((p) => {
        const candidates = [p.id, p.jid, p.lid, p.phoneNumber].filter(Boolean).map(numOnly);
        return candidates.some((c) => c && meCandidates.has(c));
      });
      const adminFlag = meEntry?.admin; // "admin" | "superadmin" | null
      const isAdmin = !!(meEntry && (adminFlag === "admin" || adminFlag === "superadmin" || meEntry.isAdmin === true || meEntry.isSuperAdmin === true));
      const announce = !!(g.announce ?? g.announcement ?? g.restrict);
      return {
        id: g.id,
        subject: g.subject || null,
        picture: null,
        participants_count: participants.length,
        isGroup: true,
        isAdmin,
        // Aliases para máxima compatibilidade com a bridge
        is_admin: isAdmin,
        iAmAdmin: isAdmin,
        announce,
        is_announcement: announce,
      };
    });
  }

  listAll() {
    return Array.from(this.sessions.values()).map((s) => ({
      id: s.id,
      status: s.lastDisconnectReason ? "disconnected" : s.status,
      phone: s.phone,
      connected: s.status === "connected" && !!s.socket && !s.lastDisconnectReason,
      hasSocket: !!s.socket,
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
  // Aceita token via header (X-Instance-Token) OU via query string (?t=...)
  // O fallback por query string é usado pelo webhook para que o consumer
  // possa baixar mídia (img/audio) direto via <img src> sem custom headers
  const token = req.headers["x-instance-token"] || req.query.t;
  const session = sessions.getByToken(token);
  if (!session) {
    return res.status(401).json({ success: false, error: "Invalid instance token" });
  }
  req.session = session;
  next();
}

app.get("/health", (_, res) => res.json({ ok: true, version: "2.4" }));

app.get("/system/metrics", (_, res) => {
  const mem = process.memoryUsage();
  res.json({
    uptime: process.uptime(),
    memory: {
      rss: mem.rss,
      heapTotal: mem.heapTotal,
      heapUsed: mem.heapUsed,
    },
    loadavg: os.loadavg(),
    sessions: sessions.listAll().length,
  });
});

app.post("/instance/create", requireAdmin, async (req, res) => {
  try {
    const { id, token } = req.body || {};
    const result = await sessions.create({ id, token });
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
    const result = await sessions.connect(req.session.id);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/disconnect", requireInstance, async (req, res) => {
  try {
    const result = await sessions.disconnect(req.session.id);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/status", requireInstance, (req, res) => {
  res.json({ success: true, ...sessions.status(req.session.id) });
});

// GET /chats — lista grupos/conversas da instância (autenticado por x-instance-token)
// Resposta: array de objetos compatível com a bridge (groups primeiro). 1:1 ainda não suportado.
app.get("/chats", requireInstance, async (req, res) => {
  try {
    const chats = await sessions.listChats(req.session.id);
    res.json(chats);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/send", requireInstance, async (req, res) => {
  try {
    // Aceita múltiplos aliases para destinatário:
    //   - phone: número BR puro (1:1)
    //   - jid / to: JID completo (@s.whatsapp.net ou @g.us)
    //   - group_jid: JID de grupo (@g.us) — atalho semântico
    const body = req.body || {};
    const recipient = body.group_jid || body.jid || body.to || body.phone;
    const { message } = body;
    if (!recipient) {
      return res.status(400).json({ success: false, error: "Missing recipient (phone | jid | to | group_jid)" });
    }
    if (!message) {
      return res.status(400).json({ success: false, error: "Missing message" });
    }
    const result = await sessions.send(req.session.id, recipient, message);
    res.json(result);
  } catch (err) {
    // Erros de envio retornam 400 (cliente) — não 200 vazio
    const msg = err.message || "send failed";
    const status = /not connected|Session not found/i.test(msg) ? 409 : 400;
    res.status(status).json({ success: false, error: msg });
  }
});

app.post("/webhook", requireInstance, (req, res) => {
  try {
    const { url } = req.body || {};
    if (!url) return res.status(400).json({ success: false, error: "url required" });
    const result = sessions.setWebhook(req.session.id, url);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
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
  console.log(`🚀 WhatsHub Engine v2.4 online na porta ${PORT}`);
  console.log(`📁 Sessões em: ${SESSIONS_DIR}`);
  await sessions.recoverPersistedSessions();
});


