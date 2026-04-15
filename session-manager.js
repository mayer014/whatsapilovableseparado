const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  downloadMediaMessage, // ✅ PASSO 1
} = require("@whiskeysockets/baileys");
const { v4: uuidv4 } = require("uuid");
const QRCode = require("qrcode");
const pino = require("pino");
const path = require("path");
const fs = require("fs");

const SESSIONS_DIR = path.join(process.cwd(), "sessions");

class SessionManager {
  constructor() {
    this.sessions = new Map();
    if (!fs.existsSync(SESSIONS_DIR)) {
      fs.mkdirSync(SESSIONS_DIR, { recursive: true });
    }
  }

  count() {
    return this.sessions.size;
  }

  getByToken(token) {
    if (!token) return null;
    for (const [, session] of this.sessions) {
      if (session.token === token) return session;
    }
    return null;
  }

  setWebhook(id, webhookUrl) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    session.webhookUrl = webhookUrl || null;
    return { success: true, webhookUrl: session.webhookUrl };
  }

  getWebhook(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    return { webhookUrl: session.webhookUrl || null };
  }

  async _notifyWebhook(session, payload) {
    if (!session.webhookUrl) return;
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);

      await fetch(session.webhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeout);
    } catch {}
  }

  async create() {
    const id = uuidv4();
    const token = uuidv4();
    const name = `WhatsApp-${id.slice(0, 6)}`;

    const session = {
      id,
      token,
      name,
      socket: null,
      status: "disconnected",
      phone: null,
      qrcode: null,
      campaigns: new Map(),
      webhookUrl: null,
      contacts: new Map(),
      chats: new Map(),
      messages: new Map(),
    };

    this.sessions.set(id, session);
    return { id, token, name };
  }

  async connect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    if (session.socket) {
      try { session.socket.end(); } catch {}
      session.socket = null;
    }

    session.status = "connecting";

    const sessionDir = path.join(SESSIONS_DIR, id);
    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
    const { version } = await fetchLatestBaileysVersion();
    const logger = pino({ level: "silent" });

    const socket = makeWASocket({
      version,
      logger,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, logger),
      },
      printQRInTerminal: false,
    });

    session.socket = socket;

    socket.ev.on("creds.update", saveCreds);

    socket.ev.on("messages.upsert", async ({ messages }) => {
      for (const msg of messages) {
        const remoteJid = msg.key.remoteJid;
        if (!remoteJid || remoteJid === "status@broadcast") continue;

        const fromMe = !!msg.key.fromMe;
        const timestamp = Date.now();

        const text = this._extractMessageText(msg);
        const type = this._extractMessageType(msg);

        this._appendMessage(session, remoteJid, {
          id: msg.key.id || uuidv4(),
          fromMe,
          text,
          timestamp,
          type,
          status: fromMe ? "sent" : "received",
          remoteJid,
          participant: msg.key.participant || null,
          rawMessage: msg, // ✅ PASSO 2
        });
      }
    });

    return new Promise((resolve) => {
      const timer = setInterval(() => {
        if (session.qrcode) {
          clearInterval(timer);
          resolve(session.qrcode);
        }
      }, 500);
    });
  }

  _appendMessage(session, jid, message) {
    const current = session.messages.get(jid) || [];
    current.push(message);
    const trimmed = current.slice(-500);
    session.messages.set(jid, trimmed);
  }

  // ✅ PASSO 3 — NOVO MÉTODO
  async downloadMedia(id, messageId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Sessão não encontrada");

    let rawMsg = null;
    for (const [, msgs] of session.messages) {
      const found = msgs.find(m => m.id === messageId);
      if (found?.rawMessage) {
        rawMsg = found.rawMessage;
        break;
      }
    }

    if (!rawMsg) {
      return { found: false, error: "Mensagem não está mais no store (expirada)" };
    }

    const buffer = await downloadMediaMessage(rawMsg, "buffer", {});

    const m = rawMsg.message || {};
    const mimetype =
      m.imageMessage?.mimetype ||
      m.audioMessage?.mimetype ||
      m.videoMessage?.mimetype ||
      m.documentMessage?.mimetype ||
      m.stickerMessage?.mimetype ||
      "application/octet-stream";

    return { found: true, buffer, mimetype };
  }

  _extractMessageText(msg) {
    const m = msg.message || {};
    return (
      m.conversation ||
      m.extendedTextMessage?.text ||
      m.imageMessage?.caption ||
      m.videoMessage?.caption ||
      ""
    );
  }

  _extractMessageType(msg) {
    const m = msg.message || {};
    if (m.conversation) return "text";
    if (m.imageMessage) return "image";
    if (m.videoMessage) return "video";
    if (m.audioMessage) return "audio";
    if (m.documentMessage) return "document";
    return "unknown";
  }
}

module.exports = { SessionManager };
