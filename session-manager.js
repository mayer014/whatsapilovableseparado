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
    this.sessions = new Map(); // id -> { id, token, name, socket, status, phone, qrcode, campaigns }
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
      syncFullHistory: false,
      browser: ["ZapMassa", "Chrome", "1.0.0"],
    });

    session.socket = socket;

    return new Promise((resolve, reject) => {
      let qrResolved = false;

      socket.ev.on("creds.update", saveCreds);

      socket.ev.on("connection.update", async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr && !qrResolved) {
          try {
            const qrDataUrl = await QRCode.toDataURL(qr);
            session.qrcode = qrDataUrl;
            qrResolved = true;
            resolve(qrDataUrl);
          } catch (err) {
            reject(err);
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

      setTimeout(() => {
        if (!qrResolved) {
          qrResolved = true;
          resolve(session.qrcode);
        }
      }, 30000);
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

    const messageId = sent?.key?.id || null;
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

  _formatJid(phone) {
    const clean = String(phone || "").replace(/\D/g, "");
    return `${clean}@s.whatsapp.net`;
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = { SessionManager };
