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

const SESSIONS_DIR = path.join(process.cwd(), "sessions");

// CONFIG PRO
const MAX_MESSAGES_PER_CHAT = 500;
const MESSAGE_TTL = 1000 * 60 * 60 * 24; // 24h
const DOWNLOAD_RETRY = 2;

class SessionManager {
  constructor() {
    this.sessions = new Map();
    if (!fs.existsSync(SESSIONS_DIR)) {
      fs.mkdirSync(SESSIONS_DIR, { recursive: true });
    }
  }

  async create() {
    const id = uuidv4();
    const token = uuidv4();

    const session = {
      id,
      token,
      socket: null,
      status: "disconnected",
      qrcode: null,

      messages: new Map(),
      messageIndex: new Map(), // ⚡ índice global
    };

    this.sessions.set(id, session);
    return { id, token };
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
        keys: makeCacheableSignalKeyStore(state.keys),
      },
      printQRInTerminal: false,
    });

    session.socket = socket;

    socket.ev.on("creds.update", saveCreds);

    socket.ev.on("messages.upsert", async ({ messages }) => {
      for (const msg of messages) {
        const jid = msg.key.remoteJid;
        if (!jid || jid === "status@broadcast") continue;

        const messageId = msg.key.id || uuidv4();
        const timestamp = Date.now();

        const rawSafe = {
          key: msg.key,
          message: msg.message,
        };

        const data = {
          id: messageId,
          timestamp,
          rawMessage: rawSafe,
        };

        // histórico do chat
        const list = session.messages.get(jid) || [];
        list.push(data);

        // limite por quantidade
        const trimmed = list.slice(-MAX_MESSAGES_PER_CHAT);

        // filtro por tempo
        const now = Date.now();
        const filtered = trimmed.filter(m => now - m.timestamp < MESSAGE_TTL);

        // 🔥 LIMPEZA DO INDEX (CORREÇÃO CRÍTICA)
        const removedIds = list
          .slice(0, list.length - filtered.length)
          .map(m => m.id);

        removedIds.forEach(rid => session.messageIndex.delete(rid));

        session.messages.set(jid, filtered);

        // index global (lookup O(1))
        session.messageIndex.set(messageId, rawSafe);
      }
    });

    return true;
  }

  // 🚀 DOWNLOAD COM RETRY + VALIDAÇÃO
  async downloadMedia(id, messageId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Sessão não encontrada");

    const rawMsg = session.messageIndex.get(messageId);

    if (!rawMsg) {
      return { found: false, error: "Mensagem não encontrada ou expirada" };
    }

    const m = rawMsg.message || {};

    const hasMedia =
      m.imageMessage ||
      m.audioMessage ||
      m.videoMessage ||
      m.documentMessage ||
      m.stickerMessage;

    if (!hasMedia) {
      return { found: false, error: "Mensagem não contém mídia" };
    }

    let buffer = null;

    for (let i = 0; i <= DOWNLOAD_RETRY; i++) {
      try {
        buffer = await downloadMediaMessage(rawMsg, "buffer", {});
        if (buffer) break;
      } catch (err) {
        if (i === DOWNLOAD_RETRY) {
          return { found: false, error: "Falha ao baixar mídia" };
        }
      }
    }

    const mimetype =
      m.imageMessage?.mimetype ||
      m.audioMessage?.mimetype ||
      m.videoMessage?.mimetype ||
      m.documentMessage?.mimetype ||
      m.stickerMessage?.mimetype ||
      "application/octet-stream";

    return { found: true, buffer, mimetype };
  }
}

module.exports = { SessionManager };
