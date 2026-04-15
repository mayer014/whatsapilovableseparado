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

    // 📩 RECEBER MENSAGENS
    socket.ev.on("messages.upsert", async ({ messages }) => {
      for (const msg of messages) {
        if (!msg.message) continue;

        const jid = msg.key.remoteJid;
        if (!jid || jid === "status@broadcast") continue;

        const messageId = msg.key.id;

        session.messageIndex.set(messageId, {
          key: msg.key,
          message: msg.message,
        });

        console.log("📩 Nova mensagem recebida");
      }
    });

    // 🔗 STATUS + QR
    socket.ev.on("connection.update", async (update) => {
      const { connection, qr, lastDisconnect } = update;

      if (qr) {
        session.qrcode = await QRCode.toDataURL(qr);
      }

      if (connection === "open") {
        session.status = "connected";
        session.qrcode = null;

        const jid = socket.user?.id;
        if (jid) {
          session.phone = jid.split("@")[0];
        }

        console.log("✅ Conectado:", id, session.phone);
      }

      if (connection === "close") {
        const shouldReconnect =
          lastDisconnect?.error?.output?.statusCode !==
          DisconnectReason.loggedOut;

        if (shouldReconnect) {
          console.log("🔄 Reconectando:", id);
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

  async sendMessage(id, { phone, message }) {
    const session = this.sessions.get(id);

    if (!session?.socket) {
      throw new Error("Instance not connected");
    }

    if (session.status !== "connected") {
      throw new Error("Instance not ready");
    }

    const clean = String(phone).replace(/\D/g, "");
    const jid = `${clean}@s.whatsapp.net`;

    const sent = await session.socket.sendMessage(jid, {
      text: message,
    });

    return {
      messageId: sent?.key?.id || null,
      to: jid,
      success: true,
    };
  }

  async downloadMedia(id, messageId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Sessão não encontrada");

    const rawMsg = session.messageIndex.get(messageId);
    if (!rawMsg) return { found: false };

    const buffer = await downloadMediaMessage(rawMsg, "buffer", {});

    return {
      found: true,
      buffer,
      mimetype: "application/octet-stream",
    };
  }
}

module.exports = { SessionManager };
