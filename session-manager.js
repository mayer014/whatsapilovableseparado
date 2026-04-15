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

// CONFIG
const MAX_MESSAGES_PER_CHAT = 500;
const MESSAGE_TTL = 1000 * 60 * 60 * 24;
const DOWNLOAD_RETRY = 2;

class SessionManager {
  constructor() {
    this.sessions = new Map();

    if (!fs.existsSync(SESSIONS_DIR)) {
      fs.mkdirSync(SESSIONS_DIR, { recursive: true });
    }
  }

  getByToken(token) {
    if (!token) return null;

    for (const [, session] of this.sessions) {
      if (session.token === token) return session;
    }

    return null;
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
      phone: null,

      messages: new Map(),
      messageIndex: new Map(),
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
      browser: ["Ubuntu", "Chrome", "20.0.04"],
      printQRInTerminal: false,
      markOnlineOnConnect: true,
    });

    session.socket = socket;
    session.status = "connecting";

    socket.ev.on("creds.update", saveCreds);

    // RECEBER MENSAGENS
    socket.ev.on("messages.upsert", async ({ messages }) => {
      for (const msg of messages) {
        if (!msg.message) continue;

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

        const list = session.messages.get(jid) || [];
        list.push(data);

        const trimmed = list.slice(-MAX_MESSAGES_PER_CHAT);

        const now = Date.now();
        const filtered = trimmed.filter(m => now - m.timestamp < MESSAGE_TTL);

        const removedIds = list
          .slice(0, list.length - filtered.length)
          .map(m => m.id);

        removedIds.forEach(id => session.messageIndex.delete(id));

        session.messages.set(jid, filtered);
        session.messageIndex.set(messageId, rawSafe);

        console.log("📩 Nova mensagem recebida");
      }
    });

    // STATUS + QR
    socket.ev.on("connection.update", async (update) => {
      const { connection, qr, lastDisconnect } = update;

      if (qr) {
        try {
          session.qrcode = await QRCode.toDataURL(qr);
        } catch (err) {
          console.error("Erro QR:", err.message);
        }
      }

      if (connection === "open") {
        session.status = "connected";
        session.qrcode = null;

        const jid = socket.user?.id;
        if (jid) {
          session.phone = jid.split(":")[0].split("@")[0];
        }

        console.log("✅ Conectado:", id, session.phone);
      }

      if (connection === "close") {
        const shouldReconnect =
          lastDisconnect?.error?.output?.statusCode !==
          DisconnectReason.loggedOut;

        if (shouldReconnect) {
          console.log("🔄 Reconectando:", id);
          session.status = "connecting";
          setTimeout(() => this.connect(id), 3000);
        } else {
          session.status = "disconnected";
          session.socket = null;
        }
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

  getStatus(id) {
    const session = this.sessions.get(id);
    if (!session) return { status: "not_found" };

    return {
      status: session.status,
      phone: session.phone,
      qrcode: session.qrcode,
    };
  }

  async disconnect(id) {
    const session = this.sessions.get(id);
    if (!session) return;

    if (session.socket) {
      try {
        await session.socket.logout();
      } catch {}
    }

    session.status = "disconnected";
    session.socket = null;
    session.qrcode = null;
    session.phone = null;
  }

  async remove(id) {
    await this.disconnect(id);
    this.sessions.delete(id);

    const sessionDir = path.join(SESSIONS_DIR, id);
    if (fs.existsSync(sessionDir)) {
      fs.rmSync(sessionDir, { recursive: true, force: true });
    }
  }

  // 🚀 ENVIO CORRIGIDO
  async sendMessage(id, { phone, message, type, mediaUrl }) {
    const session = this.sessions.get(id);

    if (!session?.socket) {
      throw new Error("Instância não conectada");
    }

    if (session.status !== "connected") {
      throw new Error("Instância não está conectada");
    }

    const clean = String(phone || "").replace(/\D/g, "");
    if (!clean) {
      throw new Error("Número inválido");
    }

    const jid = `${clean}@s.whatsapp.net`;

    console.log(`📤 Enviando para ${jid}`);

    let sent;

    try {
      if (type === "image" && mediaUrl) {
        sent = await session.socket.sendMessage(jid, {
          image: { url: mediaUrl },
          caption: message || "",
        });
      } else if (type === "video" && mediaUrl) {
        sent = await session.socket.sendMessage(jid, {
          video: { url: mediaUrl },
          caption: message || "",
        });
      } else if (type === "audio" && mediaUrl) {
        sent = await session.socket.sendMessage(jid, {
          audio: { url: mediaUrl },
          mimetype: "audio/mp4",
          ptt: true,
        });
      } else {
        sent = await session.socket.sendMessage(jid, {
          text: message,
        });
      }

      console.log("✅ Mensagem enviada");

      return {
        messageId: sent.key.id,
        to: jid,
      };

    } catch (err) {
      console.error("❌ Erro envio:", err.message);
      throw err;
    }
  }

  async downloadMedia(id, messageId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Sessão não encontrada");

    const rawMsg = session.messageIndex.get(messageId);

    if (!rawMsg) {
      return { found: false, error: "Mensagem não encontrada" };
    }

    let buffer = null;

    for (let i = 0; i <= DOWNLOAD_RETRY; i++) {
      try {
        buffer = await downloadMediaMessage(rawMsg, "buffer", {});
        if (buffer) break;
      } catch {
        if (i === DOWNLOAD_RETRY) {
          return { found: false, error: "Erro ao baixar mídia" };
        }
      }
    }

    return {
      found: true,
      buffer,
      mimetype: "application/octet-stream",
    };
  }
}

module.exports = { SessionManager };
