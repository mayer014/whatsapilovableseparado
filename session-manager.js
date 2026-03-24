const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
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

      // novos armazenamentos em memória
      contacts: new Map(), // jid -> { jid, phone, name, pushName, lastMessageAt }
      chats: new Map(),    // jid -> { jid, name, phone, lastMessage, lastMessageAt, unreadCount }
      messages: new Map(), // jid -> [ { id, fromMe, text, timestamp, type, status, remoteJid, participant } ]
    };

    this.sessions.set(id, session);
    return { id, token, name };
  }

  async connect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    if (session.socket) {
      try {
        session.socket.end();
      } catch {}
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
      generateHighQualityLinkPreview: false,
      markOnlineOnConnect: true,
      syncFullHistory: true,
      browser: ["ZapMassa", "Chrome", "1.0.0"],
    });

    session.socket = socket;

    // salvar credenciais
    socket.ev.on("creds.update", saveCreds);

    // capturar mensagens recebidas/enviadas
    socket.ev.on("messages.upsert", async ({ messages, type }) => {
      try {
        if (!messages || !Array.isArray(messages)) return;

        for (const msg of messages) {
          if (!msg.key?.remoteJid) continue;
          if (msg.key.remoteJid === "status@broadcast") continue;

          const remoteJid = msg.key.remoteJid;
          const fromMe = !!msg.key.fromMe;
          const participant = msg.key.participant || null;
          const timestamp =
            typeof msg.messageTimestamp === "number"
              ? msg.messageTimestamp * 1000
              : Date.now();

          const text = this._extractMessageText(msg);
          const messageType = this._extractMessageType(msg);
          const contactName =
            msg.pushName ||
            session.contacts.get(remoteJid)?.pushName ||
            session.contacts.get(remoteJid)?.name ||
            this._jidToPhone(remoteJid);

          this._upsertContact(session, remoteJid, {
            jid: remoteJid,
            phone: this._jidToPhone(remoteJid),
            name: contactName,
            pushName: msg.pushName || contactName,
            lastMessageAt: timestamp,
          });

          this._appendMessage(session, remoteJid, {
            id: msg.key.id || uuidv4(),
            fromMe,
            text,
            timestamp,
            type: messageType,
            status: fromMe ? "sent" : "received",
            remoteJid,
            participant,
          });

          this._upsertChat(session, remoteJid, {
            jid: remoteJid,
            phone: this._jidToPhone(remoteJid),
            name: contactName,
            lastMessage: text || `[${messageType}]`,
            lastMessageAt: timestamp,
            unreadCount: fromMe
              ? 0
              : (session.chats.get(remoteJid)?.unreadCount || 0) + 1,
          });
        }
      } catch (err) {
        console.error("messages.upsert error:", err.message);
      }
    });

    // atualizar contatos/chats a partir do WhatsApp
    socket.ev.on("contacts.update", (updates) => {
      try {
        for (const update of updates || []) {
          const jid = update.id;
          if (!jid) continue;
          const existing = session.contacts.get(jid) || {};
          session.contacts.set(jid, {
            jid,
            phone: this._jidToPhone(jid),
            name: update.notify || existing.name || this._jidToPhone(jid),
            pushName: update.notify || existing.pushName || null,
            lastMessageAt: existing.lastMessageAt || null,
          });
        }
      } catch (err) {
        console.error("contacts.update error:", err.message);
      }
    });

    socket.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          const qrDataUrl = await QRCode.toDataURL(qr);
          session.qrcode = qrDataUrl;
        } catch (err) {
          console.error("QR generation error:", err.message);
        }
      }

      if (connection === "open") {
        session.status = "connected";
        session.qrcode = null;
        const jid = socket.user?.id;
        if (jid) {
          session.phone = jid.split(":")[0].split("@")[0];
        }
        console.log(`✅ Instance ${id} connected (${session.phone})`);
      }

      if (connection === "close") {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

        if (shouldReconnect) {
          console.log(`🔄 Instance ${id} reconnecting...`);
          session.status = "connecting";
          setTimeout(() => this.connect(id), 3000);
        } else {
          session.status = "disconnected";
          session.phone = null;
          session.socket = null;
          console.log(`❌ Instance ${id} logged out`);
        }
      }
    });

    // resolve QR em até 30s
    return new Promise((resolve) => {
      const startedAt = Date.now();
      const timer = setInterval(() => {
        if (session.qrcode) {
          clearInterval(timer);
          resolve(session.qrcode);
        } else if (Date.now() - startedAt > 30000) {
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
    if (!session) throw new Error("Session not found");

    if (session.socket) {
      try {
        await session.socket.logout();
      } catch {}
      try {
        session.socket.end();
      } catch {}
      session.socket = null;
    }

    session.status = "disconnected";
    session.phone = null;
    session.qrcode = null;

    const sessionDir = path.join(SESSIONS_DIR, id);
    if (fs.existsSync(sessionDir)) {
      fs.rmSync(sessionDir, { recursive: true, force: true });
    }
  }

  async remove(id) {
    await this.disconnect(id);
    this.sessions.delete(id);
  }

  async sendMessage(id, { phone, message, type, mediaUrl }) {
    const session = this.sessions.get(id);
    if (!session?.socket) throw new Error("Not connected");
    if (session.status !== "connected") throw new Error("Instance is not connected");

    const clean = String(phone || "").replace(/\D/g, "");
    if (!clean) throw new Error("Invalid phone");

    console.log(`📤 Preparing send | instance=${id} | phone=${clean} | type=${type || "text"}`);

    const exists = await session.socket.onWhatsApp(clean);
    if (!exists || !exists.length || !exists[0]?.exists) {
      throw new Error(`Number not found on WhatsApp: ${clean}`);
    }

    const jid = exists[0].jid;
    console.log(`📍 Resolved JID | ${jid}`);

    try {
      await session.socket.presenceSubscribe(jid);
      await this._sleep(500);
    } catch (err) {
      console.log(`⚠️ presenceSubscribe failed for ${jid}: ${err.message}`);
    }

    let sent;

    if (type === "text" || !mediaUrl) {
      sent = await session.socket.sendMessage(jid, { text: message });
    } else if (type === "image") {
      sent = await session.socket.sendMessage(jid, {
        image: { url: mediaUrl },
        caption: message || "",
      });
    } else if (type === "video") {
      sent = await session.socket.sendMessage(jid, {
        video: { url: mediaUrl },
        caption: message || "",
      });
    } else if (type === "audio") {
      sent = await session.socket.sendMessage(jid, {
        audio: { url: mediaUrl },
        mimetype: "audio/mp4",
        ptt: true,
      });
    } else if (type === "document") {
      sent = await session.socket.sendMessage(jid, {
        document: { url: mediaUrl },
        mimetype: "application/pdf",
        fileName: "document.pdf",
        caption: message || "",
      });
    } else {
      throw new Error(`Unsupported message type: ${type}`);
    }

    const timestamp = Date.now();
    const messageId = sent?.key?.id || null;
    const contactName =
      session.contacts.get(jid)?.name ||
      session.contacts.get(jid)?.pushName ||
      this._jidToPhone(jid);

    this._upsertContact(session, jid, {
      jid,
      phone: this._jidToPhone(jid),
      name: contactName,
      pushName: contactName,
      lastMessageAt: timestamp,
    });

    this._appendMessage(session, jid, {
      id: messageId || uuidv4(),
      fromMe: true,
      text: message || "",
      timestamp,
      type: type || "text",
      status: "sent",
      remoteJid: jid,
      participant: null,
    });

    this._upsertChat(session, jid, {
      jid,
      phone: this._jidToPhone(jid),
      name: contactName,
      lastMessage: message || `[${type || "text"}]`,
      lastMessageAt: timestamp,
      unreadCount: 0,
    });

    console.log(`✅ Message sent | instance=${id} | jid=${jid} | messageId=${messageId}`);

    return { messageId, jid };
  }

  bulkSend(id, { folderId, phones, message, mediaUrl, type, delayMin, delayMax }) {
    const session = this.sessions.get(id);
    if (!session) return;

    const campaign = {
      folderId,
      status: "sending",
      total: phones.length,
      sent: 0,
      failed: 0,
      cancelled: false,
      paused: false,
    };
    session.campaigns.set(folderId, campaign);

    (async () => {
      for (const phone of phones) {
        if (campaign.cancelled) {
          campaign.status = "cancelled";
          break;
        }

        while (campaign.paused) {
          await this._sleep(1000);
          if (campaign.cancelled) break;
        }

        try {
          await this.sendMessage(id, { phone, message, type, mediaUrl });
          campaign.sent++;
        } catch (err) {
          campaign.failed++;
          console.error(`Failed to send to ${phone}:`, err.message);
        }

        const delay =
          Math.floor(Math.random() * (delayMax - delayMin + 1) + delayMin) * 1000;
        await this._sleep(delay);
      }

      if (campaign.status === "sending") {
        campaign.status = "completed";
      }
    })();
  }

  controlCampaign(id, folderId, action) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const campaign = session.campaigns.get(folderId);
    if (!campaign) throw new Error("Campaign not found");

    switch (action) {
      case "pause":
        campaign.paused = true;
        campaign.status = "paused";
        break;
      case "resume":
        campaign.paused = false;
        campaign.status = "sending";
        break;
      case "delete":
      case "cancel":
        campaign.cancelled = true;
        campaign.status = "cancelled";
        break;
    }

    return { status: campaign.status };
  }

  getCampaignStatus(id, folderId) {
    const session = this.sessions.get(id);
    if (!session) return { error: "Session not found" };

    const campaign = session.campaigns.get(folderId);
    if (!campaign) return { error: "Campaign not found" };

    return {
      folderId: campaign.folderId,
      status: campaign.status,
      total: campaign.total,
      sent: campaign.sent,
      failed: campaign.failed,
    };
  }

  listAll() {
    const list = [];
    for (const [, s] of this.sessions) {
      list.push({
        id: s.id,
        name: s.name,
        status: s.status,
        phone: s.phone,
      });
    }
    return list;
  }

  // NOVOS MÉTODOS PARA LEITURA

  getContacts(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    return Array.from(session.contacts.values()).sort((a, b) => {
      return (b.lastMessageAt || 0) - (a.lastMessageAt || 0);
    });
  }

  getChats(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    return Array.from(session.chats.values()).sort((a, b) => {
      return (b.lastMessageAt || 0) - (a.lastMessageAt || 0);
    });
  }

  getMessages(id, chatId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    return session.messages.get(chatId) || [];
  }

  markChatAsRead(id, chatId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const chat = session.chats.get(chatId);
    if (!chat) return { success: false, error: "Chat not found" };

    chat.unreadCount = 0;
    session.chats.set(chatId, chat);
    return { success: true };
  }

  // HELPERS INTERNOS

  _upsertContact(session, jid, data) {
    const existing = session.contacts.get(jid) || {};
    session.contacts.set(jid, {
      jid,
      phone: data.phone || existing.phone || this._jidToPhone(jid),
      name: data.name || existing.name || this._jidToPhone(jid),
      pushName: data.pushName || existing.pushName || null,
      lastMessageAt: data.lastMessageAt || existing.lastMessageAt || null,
    });
  }

  _upsertChat(session, jid, data) {
    const existing = session.chats.get(jid) || {};
    session.chats.set(jid, {
      jid,
      phone: data.phone || existing.phone || this._jidToPhone(jid),
      name: data.name || existing.name || this._jidToPhone(jid),
      lastMessage: data.lastMessage ?? existing.lastMessage ?? "",
      lastMessageAt: data.lastMessageAt || existing.lastMessageAt || Date.now(),
      unreadCount:
        typeof data.unreadCount === "number"
          ? data.unreadCount
          : existing.unreadCount || 0,
    });
  }

  _appendMessage(session, jid, message) {
    const current = session.messages.get(jid) || [];
    current.push(message);

    // limitar histórico em memória por conversa
    const trimmed = current.slice(-500);
    session.messages.set(jid, trimmed);
  }

  _jidToPhone(jid) {
    if (!jid) return "";
    return jid.split("@")[0].split(":")[0];
  }

  _extractMessageText(msg) {
    const m = msg.message || {};
    return (
      m.conversation ||
      m.extendedTextMessage?.text ||
      m.imageMessage?.caption ||
      m.videoMessage?.caption ||
      m.documentMessage?.caption ||
      m.buttonsResponseMessage?.selectedButtonId ||
      m.listResponseMessage?.title ||
      m.templateButtonReplyMessage?.selectedId ||
      ""
    );
  }

  _extractMessageType(msg) {
    const m = msg.message || {};
    if (m.conversation || m.extendedTextMessage) return "text";
    if (m.imageMessage) return "image";
    if (m.videoMessage) return "video";
    if (m.audioMessage) return "audio";
    if (m.documentMessage) return "document";
    if (m.stickerMessage) return "sticker";
    return "unknown";
  }

  _formatJid(phone) {
    const clean = String(phone || "").replace(/\D/g, "");
    return `${clean}@s.whatsapp.net`;
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = { SessionManager };
