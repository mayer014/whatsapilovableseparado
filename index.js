
// ============================================================================
// WhatsHub Engine v2.14 - Motor Baileys + boot seguro anti-reconexão fantasma
// ----------------------------------------------------------------------------
// v2.14 — Boot seguro anti-ban:
//   • NÃO reconecta sessões persistidas automaticamente no restart por padrão.
//   • Sessões com creds.json ficam restauradas em memória, aguardando /connect manual.
//   • AUTO_RECONNECT_ON_BOOT=true reativa boot reconnect, idealmente com allowlist.
//   • AUTO_RECONNECT_AFTER_CLOSE=true reativa reconexão após queda; padrão é manual.
//   • /connect é idempotente: não cria múltiplos sockets para a mesma sessão.
//   • 401/loggedOut/connection failure nunca entra em loop automático.
//
// v2.10 — Resolução canônica de JID antes do envio 1:1:
//   • Antes do send, chama sock.onWhatsApp([numero, variante BR]) para descobrir o JID real.
//   • Se nenhum candidato existir → erro 404 ("Número não encontrado no WhatsApp")
//     em vez de fingir delivered:true para um JID inexistente.
//   • Resposta passa a incluir ''resolvedFrom'' indicando qual variante foi aceita.
//   • Corrige bug v2.9 onde mensagens para variantes erradas (com/sem o "9")
//     desapareciam silenciosamente porque sendMessage sempre devolvia key.id.
//
// v2.9 — Proteção anti rate-overlimit do groupFetchAllParticipating:
//   - Cache em memória de 60s por instância (resultado de /chats)
//   - Single-flight: chamadas concorrentes compartilham a mesma Promise
//   - Circuit-breaker: 120s de cooldown servindo cache após rate-overlimit
//   - Reduz pressão no servidor do WhatsApp e protege o número de bloqueios
// v2.13 — Segurança anti-ban (jun/2026):
//   - syncFullHistory: false (era true) — não puxa mais histórico completo a
//     cada reconexão, que era rajada gigante de tráfego WS suspeita ao WhatsApp
//   - Backoff de reconexão muito mais lento (30s/2min/10min/30min/1h) com
//     teto de 5 tentativas por sessão. Acima disso, exige reconexão manual.
//   - Motivação: número sensibilizado + loops de reconexão = ban silencioso
// v2.6 — Sincronização completa de contatos (nomes da agenda do celular):
//   - syncFullHistory: true para Baileys puxar a lista completa de contatos
//   - Resolve o caso "chat aparece só com número" mesmo com contato salvo
// v2.5 — Suporte completo a chats 1:1 + grupos + histórico por JID:
//   - GET /chats devolve 1:1 e grupos com name, lastMessage, unreadCount, picture
//   - GET /messages/:jid?limit=50 devolve histórico em memória (formato Baileys)
//   - GET /contacts lista contatos conhecidos (pushName, notify)
//   - Store em memória alimentada por events de chats/contacts/messages
// v3 — Contrato completo de mídia no webhook (mantido):
//   - pushName, fromMe, isPtt
//   - mediaMimeType, mediaFileName, mediaSizeBytes, mediaDurationSeconds
//   - GET /media/:messageId com Content-Disposition (filename real)
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
const ENGINE_VERSION = "2.14";

// ─── Guardas anti-ban de reconexão ─────────────────────────────────────────
// Padrão seguro: restart do container NÃO deve abrir dezenas de WebSockets
// para sessões antigas/órfãs. A sessão fica disponível em /instance/list e
// só reconecta quando alguém chamar /connect manualmente.
const AUTO_RECONNECT_ON_BOOT = process.env.AUTO_RECONNECT_ON_BOOT === "true";
const AUTO_RECONNECT_AFTER_CLOSE = process.env.AUTO_RECONNECT_AFTER_CLOSE === "true";
const BOOT_RECONNECT_INSTANCE_IDS = new Set(
  String(process.env.BOOT_RECONNECT_INSTANCE_IDS || "")
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean)
);

function canAutoReconnectOnBoot(id) {
  if (!AUTO_RECONNECT_ON_BOOT) return false;
  return BOOT_RECONNECT_INSTANCE_IDS.size === 0 || BOOT_RECONNECT_INSTANCE_IDS.has(id);
}

function hasPersistedCreds(id) {
  return fs.existsSync(path.join(SESSIONS_DIR, id, "creds.json"));
}

if (!fs.existsSync(SESSIONS_DIR)) {
  fs.mkdirSync(SESSIONS_DIR, { recursive: true });
}

// ─── Persistência do índice de mensagens (v2.8) ────────────────────────────
// Cada sessão grava as mensagens recebidas em /app/sessions/<id>/messages.jsonl
// (uma JSON por linha). Permite que /media/:messageId continue funcionando
// após restart do container e que /messages/:jid devolva histórico real.
// Mantém no máx. MESSAGE_PERSIST_LIMIT linhas por arquivo (rotação simples).
const MESSAGE_PERSIST_LIMIT = 5000;

function messagesFilePath(sessionId) {
  return path.join(SESSIONS_DIR, sessionId, "messages.jsonl");
}

// Grava (append) uma mensagem no arquivo da sessão. Salvamos só o essencial
// (key + message + messageTimestamp + pushName) para conseguir baixar mídia
// depois com downloadMediaMessage.
function persistMessageLine(session, msg) {
  try {
    const file = messagesFilePath(session.id);
    const line = JSON.stringify({
      key: msg.key,
      message: msg.message,
      messageTimestamp: msg.messageTimestamp,
      pushName: msg.pushName || null,
    }) + "\n";
    fs.appendFileSync(file, line);

    // Rotação preguiçosa: a cada 500 mensagens persistidas verifica tamanho.
    session._persistCount = (session._persistCount || 0) + 1;
    if (session._persistCount % 500 === 0) {
      rotateMessagesFile(session.id);
    }
  } catch (err) {
    console.log(`⚠️ [${session.id}] Falha persist msg: ${err.message}`);
  }
}

function rotateMessagesFile(sessionId) {
  try {
    const file = messagesFilePath(sessionId);
    if (!fs.existsSync(file)) return;
    const lines = fs.readFileSync(file, "utf-8").split("\n").filter(Boolean);
    if (lines.length <= MESSAGE_PERSIST_LIMIT) return;
    const trimmed = lines.slice(-MESSAGE_PERSIST_LIMIT).join("\n") + "\n";
    fs.writeFileSync(file, trimmed);
    console.log(`♻️  [${sessionId}] messages.jsonl rotacionado para ${MESSAGE_PERSIST_LIMIT} linhas`);
  } catch (err) {
    console.log(`⚠️ [${sessionId}] Falha rotação: ${err.message}`);
  }
}

// Lê o arquivo da sessão e devolve as últimas N mensagens parseadas.
function readPersistedMessages(sessionId, limit = MESSAGE_PERSIST_LIMIT) {
  const file = messagesFilePath(sessionId);
  if (!fs.existsSync(file)) return [];
  try {
    const lines = fs.readFileSync(file, "utf-8").split("\n").filter(Boolean);
    const slice = lines.slice(-limit);
    const out = [];
    for (const l of slice) {
      try { out.push(JSON.parse(l)); } catch { /* ignora linha corrompida */ }
    }
    return out;
  } catch (err) {
    console.log(`⚠️ [${sessionId}] Falha ler messages.jsonl: ${err.message}`);
    return [];
  }
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

// Monta o objeto enriquecido devolvido pela API REST (/messages/:jid).
// Inclui mediaUrl absoluto + metadata, no mesmo formato do webhook,
// para que o app externo não precise montar URL nem detectar tipo.
function enrichMessageForApi(session, msg) {
  const m = msg.message || {};
  const messageType = detectMessageType(m);
  const isMedia = messageType !== "text" && messageType !== "location" && messageType !== "contact";
  const messageId = msg.key?.id;
  const mediaUrl = isMedia && messageId
    ? (PUBLIC_URL
        ? `${PUBLIC_URL}/media/${messageId}?t=${encodeURIComponent(session.token)}`
        : `/media/${messageId}`)
    : null;
  const meta = extractMediaMeta(m, messageType);
  const text =
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption ||
    m.videoMessage?.caption ||
    m.documentMessage?.caption ||
    m.documentWithCaptionMessage?.message?.documentMessage?.caption ||
    "";
  return {
    key: msg.key,
    message: msg.message,
    messageTimestamp: msg.messageTimestamp,
    pushName: msg.pushName || null,
    // Campos enriquecidos (espelham o webhook):
    messageType,
    text,
    mediaUrl,
    mediaMimeType: meta.mediaMimeType,
    mediaFileName: meta.mediaFileName,
    mediaSizeBytes: meta.mediaSizeBytes,
    mediaDurationSeconds: meta.mediaDurationSeconds,
    isPtt: meta.isPtt,
  };
}

// ---------- Gerenciador de Sessões ----------
class SessionManager {
  constructor() {
    this.sessions = new Map();
    this.reconnectAttempts = new Map();
    this.reconnectTimers = new Map();
    this.connectingPromises = new Map();
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
      // ─── Stores em memória para rotas /chats, /messages/:jid e /contacts ───
      chats: new Map(),          // jid -> { id, name, isGroup, lastMessageTs, lastMessageText, lastMessageFromMe, unreadCount, picture }
      contacts: new Map(),       // jid -> { id, name, pushName, notify }
      messagesByJid: new Map(),  // jid -> Array<{ key, message, messageTimestamp, pushName }> (máx 200/jid)
      // ─── Proteção anti rate-overlimit do groupFetchAllParticipating (v2.9) ───
      // groupsCache: último resultado bem-sucedido (mantido até nova chamada bem-sucedida).
      // groupsCacheAt: timestamp do cache.
      // groupsCooldownUntil: quando o WhatsApp respondeu rate-overlimit, bloqueia novas chamadas até esse ms.
      // groupsInflight: Promise em curso para deduplicar chamadas concorrentes (single-flight).
      groupsCache: null,
      groupsCacheAt: 0,
      groupsCooldownUntil: 0,
      groupsInflight: null,
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
        // Inicializa stores em memória (não persistidos — rebuild via events ao reconectar)
        session.chats = new Map();
        session.contacts = new Map();
        session.messagesByJid = new Map();
        session.messageIndex = new Map();
        session.groupsCache = null;
        session.groupsCacheAt = 0;
        session.groupsCooldownUntil = 0;
        session.groupsInflight = null;

        // v2.8: replay do messages.jsonl para popular messageIndex e
        // messagesByJid antes mesmo do socket reconectar. Garante que
        // /media/:messageId e /messages/:jid funcionem após restart.
        const persisted = readPersistedMessages(id);
        for (const m of persisted) {
          if (!m.key?.id || !m.message) continue;
          session.messageIndex.set(m.key.id, { key: m.key, message: m.message });
          const jid = m.key.remoteJid;
          if (jid) {
            const arr = session.messagesByJid.get(jid) || [];
            arr.push(m);
            if (arr.length > 200) arr.splice(0, arr.length - 200);
            session.messagesByJid.set(jid, arr);
          }
        }
        if (persisted.length) {
          console.log(`   📦 ${id}: ${persisted.length} msg(s) restauradas do disco`);
        }

        if (fs.existsSync(credsPath)) {
          if (canAutoReconnectOnBoot(id)) {
            console.log(`   → ${id} com credenciais, auto-reconnect de boot autorizado...`);
            this.connect(id, { reason: "boot" }).catch((err) =>
              console.log(`   ⚠️ Falha ao reconectar ${id}: ${err.message}`)
            );
          } else {
            session.lastDisconnectReason = "restored_from_disk_waiting_manual_connect";
            console.log(`   🛡️ ${id} com credenciais restauradas; aguardando /connect manual (AUTO_RECONNECT_ON_BOOT=false)`);
          }
        } else {
          console.log(`   → ${id} sem credenciais, aguardando QR`);
        }
      } catch (err) {
        console.log(`   ⚠️ Erro ao recuperar ${id}: ${err.message}`);
      }
    }
  }

  _decideReconnect(code, reason = "") {
    const text = String(reason || "").toLowerCase();

    // Padrão seguro v2.14: reconexão automática após queda fica desligada.
    // Reative só se aceitar o risco: AUTO_RECONNECT_AFTER_CLOSE=true.
    if (!AUTO_RECONNECT_AFTER_CLOSE) return false;

    const manual = [
      DisconnectReason.loggedOut,
      DisconnectReason.connectionReplaced,
      DisconnectReason.badSession,
      DisconnectReason.multideviceMismatch,
      DisconnectReason.timedOut,
    ];

    // 401/loggedOut/connection failure indica sessão inválida, banida,
    // substituída ou desconectada pelo WhatsApp. Tentar de novo só piora.
    if (manual.includes(code) || code === 401) return false;
    if (text.includes("logged out") || text.includes("connection failure")) return false;
    if (text.includes("qr refs attempts ended") || text.includes("qr") || code === 408) return false;
    return true;
  }

  _clearReconnectTimer(id) {
    const timer = this.reconnectTimers.get(id);
    if (timer) clearTimeout(timer);
    this.reconnectTimers.delete(id);
  }

  async connect(id, opts = {}) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    if (session.status === "connected" && session.socket) {
      return { id, status: "connected", reused: true };
    }

    if (session.status === "connecting" && session.socket) {
      return { id, status: "connecting", reused: true, qrcode: session.qrcode };
    }

    const inFlight = this.connectingPromises.get(id);
    if (inFlight) return inFlight;

    const promise = this._connectFresh(id, opts);
    this.connectingPromises.set(id, promise);
    try {
      return await promise;
    } finally {
      this.connectingPromises.delete(id);
    }
  }

  async _connectFresh(id, opts = {}) {
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
      // Sincroniza histórico recente (chats e mensagens) ao conectar para
      // popular as stores de chats/messages/contacts. Não puxa histórico completo.
      shouldSyncHistoryMessage: () => true,
      syncFullHistory: false, // v2.13: DESLIGADO. Evita rajada de histórico completo a cada reconexão (gatilho de ban). Use /contacts sob demanda.
    });

    session.socket = socket;
    session.status = "connecting";

    socket.ev.on("creds.update", saveCreds);

    // ─── Helpers de upsert nas stores ───
    const upsertChat = (jid, patch = {}) => {
      if (!jid || jid === "status@broadcast") return;
      const prev = session.chats.get(jid) || {
        id: jid,
        name: null,
        isGroup: jid.endsWith("@g.us"),
        lastMessageTs: 0,
        lastMessageText: null,
        lastMessageFromMe: false,
        unreadCount: 0,
        picture: null,
      };
      session.chats.set(jid, { ...prev, ...patch });
    };

    const upsertContact = (c) => {
      if (!c || !c.id) return;
      const prev = session.contacts.get(c.id) || { id: c.id };
      session.contacts.set(c.id, {
        ...prev,
        id: c.id,
        name: c.name || c.verifiedName || prev.name || null,
        pushName: c.notify || c.pushName || prev.pushName || null,
        notify: c.notify || prev.notify || null,
      });
      // Reflete nome do contato 1:1 no chat (se já existir)
      if (!c.id.endsWith("@g.us") && session.chats.has(c.id)) {
        const chat = session.chats.get(c.id);
        if (!chat.name) {
          chat.name = c.name || c.verifiedName || c.notify || chat.name;
          session.chats.set(c.id, chat);
        }
      }
    };

    const pushMessageToJid = (jid, msg) => {
      if (!jid) return;
      const arr = session.messagesByJid.get(jid) || [];
      // Evita duplicatas pelo messageId
      const id = msg.key?.id;
      if (id && arr.some((m) => m.key?.id === id)) return;
      arr.push(msg);
      // Mantém ordenado por timestamp e limita a 200
      arr.sort((a, b) => Number(a.messageTimestamp || 0) - Number(b.messageTimestamp || 0));
      if (arr.length > 200) arr.splice(0, arr.length - 200);
      session.messagesByJid.set(jid, arr);
    };

    // ─── Sync inicial de chats/contatos vindos do histórico ───
    socket.ev.on("messaging-history.set", ({ chats = [], contacts = [], messages = [] }) => {
      console.log(`📚 [${id}] history.set: ${chats.length} chats, ${contacts.length} contatos, ${messages.length} msgs`);
      for (const c of chats) {
        upsertChat(c.id, {
          name: c.name || c.subject || null,
          isGroup: c.id?.endsWith("@g.us") || false,
          unreadCount: c.unreadCount || 0,
          lastMessageTs: Number(c.conversationTimestamp || 0),
        });
      }
      for (const c of contacts) upsertContact(c);
      for (const m of messages) {
        if (!m.key?.remoteJid || !m.message) continue;
        pushMessageToJid(m.key.remoteJid, m);
      }
    });

    socket.ev.on("chats.set", ({ chats = [] }) => {
      for (const c of chats) {
        upsertChat(c.id, {
          name: c.name || c.subject || null,
          isGroup: c.id?.endsWith("@g.us") || false,
          unreadCount: c.unreadCount || 0,
          lastMessageTs: Number(c.conversationTimestamp || 0),
        });
      }
    });
    socket.ev.on("chats.upsert", (chats) => {
      for (const c of chats) {
        upsertChat(c.id, {
          name: c.name || c.subject || null,
          isGroup: c.id?.endsWith("@g.us") || false,
          unreadCount: c.unreadCount || 0,
          lastMessageTs: Number(c.conversationTimestamp || 0),
        });
      }
    });
    socket.ev.on("chats.update", (updates) => {
      for (const u of updates) {
        if (!u.id) continue;
        const patch = {};
        if (u.name || u.subject) patch.name = u.name || u.subject;
        if (u.unreadCount !== undefined) patch.unreadCount = u.unreadCount;
        if (u.conversationTimestamp) patch.lastMessageTs = Number(u.conversationTimestamp);
        upsertChat(u.id, patch);
      }
    });

    socket.ev.on("contacts.set", ({ contacts = [] }) => {
      for (const c of contacts) upsertContact(c);
    });
    socket.ev.on("contacts.upsert", (contacts) => {
      for (const c of contacts) upsertContact(c);
    });
    socket.ev.on("contacts.update", (updates) => {
      for (const c of updates) upsertContact(c);
    });

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

        if (session.messageIndex.size > MESSAGE_PERSIST_LIMIT) {
          const firstKey = session.messageIndex.keys().next().value;
          session.messageIndex.delete(firstKey);
        }

        // v2.8: persiste em disco para sobreviver a restart e permitir
        // que /media/:messageId baixe mídia antiga.
        persistMessageLine(session, msg);

        // ─── Atualiza stores de chat e mensagens por JID ───
        const ts = Number(msg.messageTimestamp || Math.floor(Date.now() / 1000));
        upsertChat(jid, {
          isGroup: jid.endsWith("@g.us"),
          lastMessageTs: ts,
          lastMessageText: text || `[${messageType}]`,
          lastMessageFromMe: !!msg.key.fromMe,
          // Incrementa unread só para mensagens recebidas
          unreadCount: msg.key.fromMe
            ? (session.chats.get(jid)?.unreadCount || 0)
            : (session.chats.get(jid)?.unreadCount || 0) + 1,
        });
        // Para 1:1 sem nome, usa pushName como fallback
        if (!jid.endsWith("@g.us") && msg.pushName) {
          const chat = session.chats.get(jid);
          if (chat && !chat.name) {
            chat.name = msg.pushName;
            session.chats.set(jid, chat);
          }
          upsertContact({ id: jid, notify: msg.pushName });
        }
        pushMessageToJid(jid, {
          key: msg.key,
          message: msg.message,
          messageTimestamp: ts,
          pushName: msg.pushName || null,
        });

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

        // v2.13: backoff bem mais lento + teto de 5 tentativas por sessão.
        // Reconexões frequentes em curto intervalo são um dos principais
        // gatilhos de banimento. Preferimos parar e exigir reconexão manual
        // (novo QR) a entupir a sessão do WhatsApp com tentativas.
        const MAX_RECONNECT_ATTEMPTS = 5;
        const RECONNECT_DELAYS_MS = [30_000, 120_000, 600_000, 1_800_000, 3_600_000]; // 30s, 2min, 10min, 30min, 1h
        if (shouldReconnect) {
          const attempts = (this.reconnectAttempts.get(id) || 0) + 1;
          this.reconnectAttempts.set(id, attempts);
          if (attempts > MAX_RECONNECT_ATTEMPTS) {
            console.log(`⛔ [${id}] Limite de ${MAX_RECONNECT_ATTEMPTS} tentativas atingido. Pare automática. Reconecte manualmente.`);
            session.status = "disconnected";
            session.lastDisconnectReason = `max_reconnect_attempts_reached (${attempts})`;
            this._clearReconnectTimer(id);
          } else {
            const delay = RECONNECT_DELAYS_MS[attempts - 1] || 3_600_000;
            console.log(`🔄 [${id}] Tentativa #${attempts}/${MAX_RECONNECT_ATTEMPTS} em ${Math.round(delay/1000)}s...`);
            const timer = setTimeout(() => {
              this.reconnectTimers.delete(id);
              this.connect(id).catch((err) =>
                console.log(`⚠️ [${id}] Reconexão falhou: ${err.message}`)
              );
            }, delay);
            this.reconnectTimers.set(id, timer);
          }
        } else {
          session.status = "disconnected";
          this._clearReconnectTimer(id);
          console.log(`⛔ [${id}] Reconexão automática desabilitada (${code})`);
        }
      }
    });

    return { id, status: session.status, reason: opts.reason || "manual" };
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

  // ─── Limpeza de sessões órfãs ───────────────────────────────────────────
  // Encerra socket, remove da memória e apaga a pasta em /app/sessions/<id>.
  // Usado por /sessions/cleanup para liberar RAM/disco de instâncias que não
  // existem mais no banco do hub.
  async removeSession(id) {
    const session = this.sessions.get(id);
    if (session && session.socket) {
      try { await session.socket.logout(); } catch {}
      try { session.socket.end?.(); } catch {}
      session.socket = null;
    }
    this._clearReconnectTimer(id);
    this.sessions.delete(id);
    this.reconnectAttempts.delete(id);
    this.connectingPromises.delete(id);

    const dir = path.join(SESSIONS_DIR, id);
    if (fs.existsSync(dir)) {
      try { fs.rmSync(dir, { recursive: true, force: true }); } catch (err) {
        console.log(`   ⚠️ Falha ao remover ${dir}: ${err.message}`);
      }
    }
    return { id, removed: true };
  }

  // Recebe a lista de IDs que devem permanecer. Tudo o que não estiver nela
  // (tanto na memória quanto no disco) é removido. Retorna o que foi limpo.
  async cleanupOrphans(keepIds) {
    const keep = new Set((keepIds || []).map(String));
    const removed = [];

    // 1) Pastas no disco que não estão na lista
    if (fs.existsSync(SESSIONS_DIR)) {
      const dirs = fs.readdirSync(SESSIONS_DIR).filter((d) =>
        fs.statSync(path.join(SESSIONS_DIR, d)).isDirectory()
      );
      for (const id of dirs) {
        if (!keep.has(id)) {
          await this.removeSession(id);
          removed.push(id);
        }
      }
    }

    // 2) Sessões só-em-memória (sem pasta) que não estão na lista
    for (const id of Array.from(this.sessions.keys())) {
      if (!keep.has(id) && !removed.includes(id)) {
        await this.removeSession(id);
        removed.push(id);
      }
    }

    return { kept: keep.size, removed: removed.length, removedIds: removed };
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

    // ----- Envio 1:1 (v2.10: canonicaliza JID via onWhatsApp antes de enviar) -----
    // PROBLEMA v2.9: sendMessage sempre retorna key.id, mesmo para JIDs inexistentes.
    // Mensagens para variantes erradas (ex: com/sem o "9") sumiam silenciosamente.
    // FIX v2.10: usa sock.onWhatsApp([candidatos]) para descobrir o JID real cadastrado
    // no WhatsApp ANTES de chamar sendMessage. Se nenhum candidato existir → erro 404.
    let phone;
    let inputJid = null;

    if (isUserJid) {
      inputJid = raw;
      phone = raw.split("@")[0].split(":")[0].replace(/\D/g, "");
    } else {
      phone = raw.replace(/\D/g, "");
      if (!phone.startsWith("55") || phone.length < 12 || phone.length > 13) {
        throw new Error(
          `Número inválido: "${recipientRaw}". Use formato 55 + DDD + número (ex: 5567999999999) ou JID @g.us para grupos`
        );
      }
    }

    // Monta lista de candidatos (número original + variante BR com/sem "9")
    const candidates = [];
    if (phone) candidates.push(phone);
    const alt = tryBrazilianAlternative(phone);
    if (alt && !candidates.includes(alt)) candidates.push(alt);

    // Resolve via onWhatsApp para obter JID canônico
    let canonicalJid = null;
    let resolvedFrom = null;
    try {
      const checks = await session.socket.onWhatsApp(...candidates);
      // Baileys retorna [{ jid, exists }, ...]
      for (const c of (checks || [])) {
        if (c && c.exists && c.jid) {
          canonicalJid = c.jid;
          resolvedFrom = c.jid.split("@")[0];
          break;
        }
      }
    } catch (err) {
      console.log(`⚠️ [${id}] onWhatsApp falhou: ${err.message}`);
    }

    // Fallback: se onWhatsApp falhou (rede/timeout) mas temos JID já formatado pelo cliente, tenta esse
    if (!canonicalJid && inputJid) {
      console.log(`⚠️ [${id}] onWhatsApp não respondeu — usando JID fornecido pelo cliente como fallback`);
      canonicalJid = inputJid;
    }

    if (!canonicalJid) {
      throw new Error(
        `Número não encontrado no WhatsApp: ${candidates.join(" / ")}. Verifique se o destinatário possui WhatsApp ativo.`
      );
    }

    let sent = null;
    let delivered = false;
    try {
      sent = await session.socket.sendMessage(canonicalJid, { text: String(message) });
      delivered = !!sent?.key?.id;
    } catch (err) {
      console.log(`⚠️ [${id}] sendMessage falhou para ${canonicalJid}: ${err.message}`);
      throw new Error(`Falha ao enviar para ${canonicalJid}: ${err.message}`);
    }

    if (!delivered) {
      throw new Error("Envio não confirmado pela rede WhatsApp (sem messageId)");
    }

    console.log(`📤 [${id}] → ${canonicalJid} (resolvido de ${resolvedFrom || phone}): ${String(message).slice(0, 50)}`);

    return {
      success: true,
      delivered: true,
      messageId: sent?.key?.id || null,
      to: canonicalJid,
      resolvedFrom: resolvedFrom || phone,
    };
  }

  // ─── Envio de mídia (v2.11) ────────────────────────────────────────────────
  // Baixa a URL pública informada e envia via Baileys como image/video/audio/document.
  // Reusa a mesma resolução de JID do send() (grupo direto, 1:1 via onWhatsApp).
  async sendMedia(id, recipientRaw, mediaUrl, caption, mediaType, fileName) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    if (session.status !== "connected" || !session.socket) {
      throw new Error("Instance not connected");
    }

    const raw = String(recipientRaw || "").trim();
    if (!raw) throw new Error("Destinatário vazio");
    if (!mediaUrl) throw new Error("mediaUrl é obrigatório");

    const type = String(mediaType || "image").toLowerCase();
    if (!["image", "video", "audio", "document"].includes(type)) {
      throw new Error(`mediaType inválido: ${type}. Use image|video|audio|document`);
    }

    // ----- Resolve o JID destinatário (mesma lógica do send) -----
    const isGroupJid = raw.endsWith("@g.us");
    const isUserJid = raw.endsWith("@s.whatsapp.net");
    let canonicalJid = null;
    let resolvedFrom = null;

    if (isGroupJid) {
      canonicalJid = raw;
    } else {
      let phone, inputJid = null;
      if (isUserJid) {
        inputJid = raw;
        phone = raw.split("@")[0].split(":")[0].replace(/\D/g, "");
      } else {
        phone = raw.replace(/\D/g, "");
        if (!phone.startsWith("55") || phone.length < 12 || phone.length > 13) {
          throw new Error(`Número inválido: "${recipientRaw}". Use formato 55 + DDD + número`);
        }
      }
      const candidates = [phone];
      const alt = tryBrazilianAlternative(phone);
      if (alt && !candidates.includes(alt)) candidates.push(alt);
      try {
        const checks = await session.socket.onWhatsApp(...candidates);
        for (const c of (checks || [])) {
          if (c && c.exists && c.jid) {
            canonicalJid = c.jid;
            resolvedFrom = c.jid.split("@")[0];
            break;
          }
        }
      } catch (err) {
        console.log(`⚠️ [${id}] onWhatsApp falhou (media): ${err.message}`);
      }
      if (!canonicalJid && inputJid) canonicalJid = inputJid;
      if (!canonicalJid) {
        throw new Error(`Número não encontrado no WhatsApp: ${candidates.join(" / ")}`);
      }
    }

    // ----- Monta o payload de mídia para o Baileys -----
    const mediaContent = { url: String(mediaUrl) };
    let payload;
    if (type === "image") {
      payload = { image: mediaContent, caption: caption ? String(caption) : undefined };
    } else if (type === "video") {
      payload = { video: mediaContent, caption: caption ? String(caption) : undefined };
    } else if (type === "audio") {
      payload = { audio: mediaContent, mimetype: "audio/ogg; codecs=opus", ptt: true };
    } else {
      payload = {
        document: mediaContent,
        fileName: fileName ? String(fileName) : "arquivo",
        caption: caption ? String(caption) : undefined,
      };
    }

    let sent;
    try {
      sent = await session.socket.sendMessage(canonicalJid, payload);
    } catch (err) {
      console.log(`⚠️ [${id}] sendMessage(media) falhou para ${canonicalJid}: ${err.message}`);
      throw new Error(`Falha ao enviar mídia para ${canonicalJid}: ${err.message}`);
    }

    const delivered = !!sent?.key?.id;
    if (!delivered) throw new Error("Envio de mídia não confirmado (sem messageId)");

    console.log(`📤 [${id}] → ${canonicalJid} (${type}): ${String(mediaUrl).slice(0, 80)}`);
    return {
      success: true,
      delivered: true,
      messageId: sent.key.id,
      to: canonicalJid,
      mediaType: type,
      isGroup: isGroupJid,
      resolvedFrom: resolvedFrom || undefined,
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
  // Combina grupos (groupFetchAllParticipating) + 1:1 (store em memória).
  // Retorna payload normalizado: id, name, isGroup, lastMessage, unreadCount, picture, participants_count, isAdmin.
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

    // ─── Grupos (fonte autoritativa: groupFetchAllParticipating) ───
    // ───────────────────────────────────────────────────────────────────────
    // PROTEÇÃO v2.9: cache 60s + circuit-breaker em rate-overlimit + single-flight.
    // Motivo: groupFetchAllParticipating é uma chamada PESADA do servidor do WhatsApp.
    // Sem proteção, apps externos chamando /chats em loop derrubam o número em "rate-overlimit"
    // e, em volume sustentado, podem causar bloqueio temporário/permanente daquele WhatsApp.
    // Estratégia: o motor só bate no WhatsApp 1x a cada 60s POR INSTÂNCIA, mesmo com N
    // requisições concorrentes vindas dos consumidores.
    // ───────────────────────────────────────────────────────────────────────
    const GROUPS_CACHE_TTL_MS = 60 * 1000;        // 1 min de cache fresco
    const GROUPS_COOLDOWN_MS = 120 * 1000;        // 2 min de bloqueio após rate-overlimit
    const now = Date.now();

    let groupsMap = {};
    const cacheStillFresh = session.groupsCache && (now - session.groupsCacheAt) < GROUPS_CACHE_TTL_MS;
    const inCooldown = now < session.groupsCooldownUntil;

    if (cacheStillFresh) {
      // Cache fresco — devolve sem bater no WhatsApp.
      groupsMap = session.groupsCache;
    } else if (inCooldown) {
      // Estamos em cooldown pós rate-overlimit. Devolve cache antigo (mesmo expirado) ou vazio.
      // Importante: NÃO logar a cada chamada — só no início do cooldown (já foi logado).
      groupsMap = session.groupsCache || {};
    } else {
      // Single-flight: se já tem uma chamada em curso, aguarda ela em vez de disparar outra.
      if (!session.groupsInflight) {
        session.groupsInflight = (async () => {
          try {
            const fresh = await sock.groupFetchAllParticipating();
            session.groupsCache = fresh;
            session.groupsCacheAt = Date.now();
            return fresh;
          } catch (err) {
            const msg = err?.message || String(err);
            const isRateLimit = /rate-overlimit|rate.?limit/i.test(msg);
            if (isRateLimit) {
              session.groupsCooldownUntil = Date.now() + GROUPS_COOLDOWN_MS;
              console.log(`⚠️ [${id}] groupFetchAllParticipating: rate-overlimit do WhatsApp — cooldown de ${GROUPS_COOLDOWN_MS / 1000}s ativado. Servindo cache.`);
            } else {
              console.log(`⚠️ [${id}] groupFetchAllParticipating falhou: ${msg}`);
            }
            return session.groupsCache || {};
          } finally {
            session.groupsInflight = null;
          }
        })();
      }
      groupsMap = await session.groupsInflight;
    }

    const groupChats = Object.values(groupsMap).map((g) => {
      const participants = g.participants || [];
      const meEntry = participants.find((p) => {
        const candidates = [p.id, p.jid, p.lid, p.phoneNumber].filter(Boolean).map(numOnly);
        return candidates.some((c) => c && meCandidates.has(c));
      });
      const adminFlag = meEntry?.admin;
      const isAdmin = !!(meEntry && (adminFlag === "admin" || adminFlag === "superadmin" || meEntry.isAdmin === true || meEntry.isSuperAdmin === true));
      const announce = !!(g.announce ?? g.announcement ?? g.restrict);
      // Mescla com a store em memória (lastMessage, unreadCount)
      const stored = session.chats.get(g.id) || {};
      // Garante que o nome do grupo fique salvo na store
      if (g.subject) {
        session.chats.set(g.id, {
          ...stored,
          id: g.id,
          name: g.subject,
          isGroup: true,
        });
      }
      return {
        id: g.id,
        name: g.subject || stored.name || null,
        // Mantido por compat: alguns clientes ainda leem "subject"
        subject: g.subject || null,
        isGroup: true,
        picture: null,
        participants_count: participants.length,
        isAdmin,
        is_admin: isAdmin,
        iAmAdmin: isAdmin,
        announce,
        is_announcement: announce,
        unreadCount: stored.unreadCount || 0,
        lastMessage: stored.lastMessageTs ? {
          text: stored.lastMessageText,
          timestamp: stored.lastMessageTs,
          fromMe: stored.lastMessageFromMe,
        } : null,
      };
    });

    // ─── 1:1 (vem da store em memória, alimentada por messages.upsert + history.set) ───
    const directChats = [];
    for (const [jid, chat] of session.chats.entries()) {
      if (jid.endsWith("@g.us") || jid.endsWith("@broadcast") || jid.endsWith("@lid")) continue;
      // Só conta como 1:1 quem tem domínio @s.whatsapp.net
      if (!jid.endsWith("@s.whatsapp.net")) continue;
      const contact = session.contacts.get(jid);
      directChats.push({
        id: jid,
        name: chat.name || contact?.name || contact?.pushName || contact?.notify || null,
        isGroup: false,
        picture: null,
        unreadCount: chat.unreadCount || 0,
        lastMessage: chat.lastMessageTs ? {
          text: chat.lastMessageText,
          timestamp: chat.lastMessageTs,
          fromMe: chat.lastMessageFromMe,
        } : null,
      });
    }

    // Ordena por timestamp da última mensagem (desc); chats sem msg vão pro fim
    const all = [...groupChats, ...directChats];
    all.sort((a, b) => (b.lastMessage?.timestamp || 0) - (a.lastMessage?.timestamp || 0));
    return all;
  }

  // Lista mensagens de um JID específico (formato Baileys), lidas da store em memória.
  // Não chama a rede — devolve o histórico já capturado por messages.upsert + history.set.
  listMessages(id, jid, limit = 50) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    const arr = session.messagesByJid.get(jid) || [];
    const lim = Math.max(1, Math.min(Number(limit) || 50, 200));
    // Devolve as últimas N, em ordem cronológica ascendente
    return arr.slice(-lim);
  }

  // Lista contatos conhecidos da sessão.
  listContacts(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    return Array.from(session.contacts.values());
  }

  // ─── v2.12: Listagem de grupos para apps externos (Ponte API) ───
  // Usa o cache de groupFetchAllParticipating já mantido por listChats (TTL 60s + circuit-breaker).
  async listGroups(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    if (session.status !== "connected" || !session.socket) {
      throw new Error("Instance not connected");
    }
    const sock = session.socket;
    const now = Date.now();
    const GROUPS_CACHE_TTL_MS = 60 * 1000;
    const GROUPS_COOLDOWN_MS = 120 * 1000;

    // Reaproveita cache se fresco; senão dispara fetch com single-flight + cooldown.
    let groupsMap;
    const cacheFresh = session.groupsCache && (now - session.groupsCacheAt) < GROUPS_CACHE_TTL_MS;
    const inCooldown = now < session.groupsCooldownUntil;
    if (cacheFresh) {
      groupsMap = session.groupsCache;
    } else if (inCooldown) {
      groupsMap = session.groupsCache || {};
    } else {
      if (!session.groupsInflight) {
        session.groupsInflight = (async () => {
          try {
            const fresh = await sock.groupFetchAllParticipating();
            session.groupsCache = fresh;
            session.groupsCacheAt = Date.now();
            return fresh;
          } catch (err) {
            const msg = err?.message || String(err);
            if (/rate-overlimit|rate.?limit/i.test(msg)) {
              session.groupsCooldownUntil = Date.now() + GROUPS_COOLDOWN_MS;
              console.log(`⚠️ [${id}] listGroups: rate-overlimit — cooldown ${GROUPS_COOLDOWN_MS/1000}s`);
            } else {
              console.log(`⚠️ [${id}] groupFetchAllParticipating falhou: ${msg}`);
            }
            return session.groupsCache || {};
          } finally {
            session.groupsInflight = null;
          }
        })();
      }
      groupsMap = await session.groupsInflight;
    }

    const numOnly = (jid) => (jid && typeof jid === "string") ? jid.split("@")[0].split(":")[0].replace(/\D/g, "") : "";
    const meSet = new Set();
    if (sock.user?.id) meSet.add(numOnly(sock.user.id));
    if (sock.user?.lid) meSet.add(numOnly(sock.user.lid));
    if (session.phone) meSet.add(numOnly(session.phone));

    const groups = Object.values(groupsMap).map((g) => {
      const participants = g.participants || [];
      const meEntry = participants.find((p) => {
        const candidates = [p.id, p.jid, p.lid].filter(Boolean).map(numOnly);
        return candidates.some((c) => c && meSet.has(c));
      });
      const adminFlag = meEntry?.admin;
      const isAdmin = !!(meEntry && (adminFlag === "admin" || adminFlag === "superadmin"));
      return {
        id: g.id,
        subject: g.subject || null,
        size: participants.length,
        owner: g.owner || null,
        creation: g.creation || null,
        desc: g.desc || null,
        is_announcement: !!(g.announce ?? g.announcement ?? g.restrict),
        is_admin: isAdmin,
      };
    });
    return { count: groups.length, groups };
  }

  // Metadados completos de UM grupo. Aceita JID com @g.us.
  // Tenta resolver número real mesmo quando participante vem como @lid.
  async getGroupParticipants(id, groupJid) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    if (session.status !== "connected" || !session.socket) {
      throw new Error("Instance not connected");
    }
    if (!groupJid || !groupJid.endsWith("@g.us")) {
      throw new Error("Invalid group_jid (must end with @g.us)");
    }
    const sock = session.socket;
    const meta = await sock.groupMetadata(groupJid);

    const numOnly = (jid) => (jid && typeof jid === "string") ? jid.split("@")[0].split(":")[0].replace(/\D/g, "") : "";

    const participants = (meta.participants || []).map((p) => {
      const rawId = p.id || "";
      const rawLid = p.lid || "";
      const isLidOnly = rawId.endsWith("@lid");
      // Para participantes com domínio @s.whatsapp.net, o número é direto.
      // Para @lid, tentamos achar o número real via cache de contatos da sessão.
      let phone_e164 = null;
      if (rawId.endsWith("@s.whatsapp.net")) {
        phone_e164 = numOnly(rawId);
      } else if (rawLid && rawLid.endsWith("@s.whatsapp.net")) {
        phone_e164 = numOnly(rawLid);
      } else {
        // Tenta resolver via contatos conhecidos (alguns chegam com pushName + número real)
        const contactByLid = session.contacts.get(rawId);
        if (contactByLid?.id?.endsWith("@s.whatsapp.net")) {
          phone_e164 = numOnly(contactByLid.id);
        }
      }
      return {
        id: rawId,
        lid: rawLid || null,
        admin: p.admin || null,
        phone_e164,
        lid_only: isLidOnly && !phone_e164,
      };
    });

    return {
      id: meta.id,
      subject: meta.subject || null,
      owner: meta.owner || null,
      creation: meta.creation || null,
      desc: meta.desc || null,
      size: participants.length,
      participants,
    };
  }

  listAll() {
    return Array.from(this.sessions.values()).map((s) => ({
      id: s.id,
      status: s.lastDisconnectReason ? "disconnected" : s.status,
      phone: s.phone,
      connected: s.status === "connected" && !!s.socket && !s.lastDisconnectReason,
      hasSocket: !!s.socket,
      webhook: s.webhook,
      hasCreds: hasPersistedCreds(s.id),
      reconnectAttempts: this.reconnectAttempts.get(s.id) || 0,
      reconnectScheduled: this.reconnectTimers.has(s.id),
      bootAutoReconnectEnabled: AUTO_RECONNECT_ON_BOOT,
      closeAutoReconnectEnabled: AUTO_RECONNECT_AFTER_CLOSE,
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

app.get("/health", (_, res) => res.json({ ok: true, version: ENGINE_VERSION }));

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

// POST /sessions/cleanup (admin) — recebe { keep: [id, ...] } e remove
// da memória + disco toda sessão que não estiver na lista. Devolve a
// quantidade removida e os IDs afetados. Útil para liberar RAM de
// instâncias órfãs (deletadas no hub mas que ficaram no /app/sessions).
app.post("/sessions/cleanup", requireAdmin, async (req, res) => {
  try {
    const { keep } = req.body || {};
    if (!Array.isArray(keep)) {
      return res.status(400).json({ success: false, error: "keep[] required" });
    }
    const result = await sessions.cleanupOrphans(keep);
    console.log(`🧹 Cleanup: mantidas ${result.kept}, removidas ${result.removed}`);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
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
// Resposta: array com grupos + conversas 1:1. Cada item tem { id, name, isGroup,
// lastMessage, unreadCount, picture }. Ordenado por última mensagem desc.
app.get("/chats", requireInstance, async (req, res) => {
  try {
    const chats = await sessions.listChats(req.session.id);
    res.json(chats);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET /messages/:jid?limit=50 — histórico de mensagens de um JID (formato Baileys).
// Lê da store em memória (alimentada por messages.upsert + history.set).
// Útil para o cliente abrir um chat e exibir o histórico recente sem polling.
app.get("/messages/:jid", requireInstance, (req, res) => {
  try {
    const jid = decodeURIComponent(req.params.jid);
    const limit = req.query.limit ? Number(req.query.limit) : 50;
    const messages = sessions.listMessages(req.session.id, jid, limit);
    // v2.8: enriquece cada mensagem com mediaUrl absoluto + metadata
    // para o app externo poder baixar imagem/áudio direto via <img>/<audio>.
    const enriched = messages.map((m) => enrichMessageForApi(req.session, m));
    res.json({ jid, count: enriched.length, messages: enriched });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET /contacts — lista contatos conhecidos (pushName/notify) da sessão.
// Útil para resolver nome de 1:1 que ainda não trocou mensagem.
app.get("/contacts", requireInstance, (req, res) => {
  try {
    const contacts = sessions.listContacts(req.session.id);
    res.json({ count: contacts.length, contacts });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET /groups — lista grupos da instância (v2.12). Resposta: { count, groups: [...] }
// Cache 60s + single-flight (proteção contra rate-limit do WhatsApp).
app.get("/groups", requireInstance, async (req, res) => {
  try {
    const result = await sessions.listGroups(req.session.id);
    res.json({ success: true, ...result });
  } catch (err) {
    const msg = err.message || "listGroups failed";
    const status = /not connected|Session not found/i.test(msg) ? 409 : 500;
    res.status(status).json({ success: false, error: msg });
  }
});

// GET /groups/:jid/participants — metadados completos de UM grupo (v2.12).
// Resposta: { id, subject, owner, creation, desc, size, participants: [...] }
// Cada participante traz { id, lid, admin, phone_e164, lid_only }.
// IMPORTANTE: throttle ~400ms se o cliente chamar em loop (proteção anti-ban).
app.get("/groups/:jid/participants", requireInstance, async (req, res) => {
  try {
    const jid = decodeURIComponent(req.params.jid);
    const result = await sessions.getGroupParticipants(req.session.id, jid);
    // Pequeno delay para desencorajar varreduras agressivas
    await new Promise((r) => setTimeout(r, 400));
    res.json({ success: true, ...result });
  } catch (err) {
    const msg = err.message || "groupMetadata failed";
    const status = /not connected|Session not found|Invalid group_jid/i.test(msg) ? 400 : 500;
    res.status(status).json({ success: false, error: msg });
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

// ─── POST /send-media (v2.11) ─────────────────────────────────────────────
// Payload: { phone|jid|to|group_jid, mediaUrl, caption?, mediaType?, fileName? }
// mediaType: image (default) | video | audio | document
app.post("/send-media", requireInstance, async (req, res) => {
  try {
    const body = req.body || {};
    const recipient = body.group_jid || body.jid || body.to || body.phone;
    const mediaUrl = body.mediaUrl || body.media_url || body.url;
    const caption = body.caption || "";
    const mediaType = body.mediaType || body.media_type || "image";
    const fileName = body.fileName || body.file_name || body.filename;

    if (!recipient) {
      return res.status(400).json({ success: false, error: "Missing recipient (phone | jid | to | group_jid)" });
    }
    if (!mediaUrl) {
      return res.status(400).json({ success: false, error: "Missing mediaUrl" });
    }

    const result = await sessions.sendMedia(req.session.id, recipient, mediaUrl, caption, mediaType, fileName);
    res.json(result);
  } catch (err) {
    const msg = err.message || "send-media failed";
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
  console.log(`🚀 WhatsHub Engine v${ENGINE_VERSION} online na porta ${PORT}`);
  console.log(`📁 Sessões em: ${SESSIONS_DIR}`);
  await sessions.recoverPersistedSessions();
});
