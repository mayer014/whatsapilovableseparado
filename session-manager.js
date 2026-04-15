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

  // ═══════════════════════════════════════════════════════════════
  // WEBHOOK — Envia eventos para URL configurada
  // ═══════════════════════════════════════════════════════════════
  /**
   * Configura o webhook URL para uma instância.
   * Quando configurado, mensagens e eventos de conexão são enviados
   * automaticamente para esta URL via POST.
   */
  setWebhook(id, webhookUrl) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    session.webhookUrl = webhookUrl || null;
    console.log(`🔗 Webhook ${webhookUrl ? 'set' : 'removed'} for instance ${id}: ${webhookUrl || 'none'}`);
    return { success: true, webhookUrl: session.webhookUrl };
  }

  getWebhook(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    return { webhookUrl: session.webhookUrl || null };
  }

  /**
   * Envia evento ao webhook configurado (fire-and-forget).
   * Nunca bloqueia o fluxo principal se falhar.
   */
  async _notifyWebhook(session, payload) {
    if (!session.webhookUrl) return;
    
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      
      const response = await fetch(session.webhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      clearTimeout(timeout);
      
      const responseText = await response.text().catch(() => "");
      console.log(`✅ Webhook POST → ${session.webhookUrl} — status: ${response.status}`);
      
      if (!response.ok) {
        console.error(`❌ Webhook error response: ${response.status} — ${responseText.substring(0, 200)}`);
      }
    } catch (err) {
      console.error(`❌ Webhook POST failed → ${session.webhookUrl}:`, err.message);
    }
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
      webhookUrl: null, // URL para envio de eventos via webhook

      // armazenamentos em memória
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

    // ═══════════════════════════════════════════════════════════════
    // CAPTURA DE MENSAGENS + ENVIO AO WEBHOOK
    // ═══════════════════════════════════════════════════════════════
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

          // ═══════════════════════════════════════════════════════
          // WEBHOOK: Enviar mensagem recebida ao CRM
          // ═══════════════════════════════════════════════════════
          if (session.webhookUrl) {
            const webhookPayload = {
              event: "message",
              from: this._jidToPhone(remoteJid),
              message: text || `[${messageType}]`,
              pushName: msg.pushName || contactName,
              fromMe,
              instance_id: session.id,
              timestamp: new Date(timestamp).toISOString(),
            };

            // Adicionar info de mídia
            if (messageType === "image") {
              webhookPayload.media_type = "image";
              webhookPayload.caption = msg.message?.imageMessage?.caption;
            } else if (messageType === "audio") {
              webhookPayload.media_type = "audio";
            } else if (messageType === "video") {
              webhookPayload.media_type = "video";
              webhookPayload.caption = msg.message?.videoMessage?.caption;
            } else if (messageType === "document") {
              webhookPayload.media_type = "document";
              webhookPayload.filename = msg.message?.documentMessage?.fileName;
            }

            console.log(`📨 Webhook: message from ${webhookPayload.from} (fromMe=${fromMe})`);
            this._notifyWebhook(session, webhookPayload);
          }
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

    // ═══════════════════════════════════════════════════════════════
    // CONEXÃO + WEBHOOK DE STATUS
    // ═══════════════════════════════════════════════════════════════
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

        // WEBHOOK: Notificar conexão bem-sucedida
        this._notifyWebhook(session, {
          event: "connected",
          instance_id: session.id,
          phone: session.phone,
          timestamp: new Date().toISOString(),
        });
      }

      if (connection === "close") {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

        // WEBHOOK: Notificar desconexão
        this._notifyWebhook(session, {
          event: "disconnected",
          instance_id: session.id,
          reason: shouldReconnect ? "temporary" : "logged_out",
          statusCode,
          timestamp: new Date().toISOString(),
        });

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
      text: message || `[${type}]`,
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
      lastMessage: message || `[${type}]`,
      lastMessageAt: timestamp,
      unreadCount: 0,
    });

    return { messageId, to: jid, timestamp };
  }

  bulkSend(id, { folderId, phones, message, mediaUrl, type, delayMin, delayMax }) {
    const session = this.sessions.get(id);
    if (!session?.socket) throw new Error("Not connected");

    const campaign = {
      folderId,
      status: "running",
      total: phones.length,
      sent: 0,
      failed: 0,
      errors: [],
      startedAt: Date.now(),
      controller: new AbortController(),
    };

    session.campaigns.set(folderId, campaign);

    (async () => {
      for (const phone of phones) {
        if (campaign.status === "paused") {
          while (campaign.status === "paused") {
            await this._sleep(500);
          }
        }
        if (campaign.controller.signal.aborted) break;

        try {
          await this.sendMessage(id, { phone, message, type, mediaUrl });
          campaign.sent++;
        } catch (err) {
          campaign.failed++;
          campaign.errors.push({ phone, error: err.message });
        }

        const delay = Math.random() * (delayMax - delayMin) + delayMin;
        await this._sleep(delay * 1000);
      }

      if (!campaign.controller.signal.aborted) {
        campaign.status = "completed";
      }
    })();

    return { folderId, status: "running", total: phones.length };
  }

  controlCampaign(id, folderId, action) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const campaign = session.campaigns.get(folderId);
    if (!campaign) throw new Error("Campaign not found");

    switch (action) {
      case "pause":
        campaign.status = "paused";
        break;
      case "resume":
        campaign.status = "running";
        break;
      case "delete":
        campaign.controller.abort();
        campaign.status = "deleted";
        break;
      default:
        throw new Error("Invalid action: use pause, resume, or delete");
    }

    return { folderId, status: campaign.status };
  }

  getCampaignStatus(id, folderId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const campaign = session.campaigns.get(folderId);
    if (!campaign) throw new Error("Campaign not found");

    return {
      folderId: campaign.folderId,
      status: campaign.status,
      total: campaign.total,
      sent: campaign.sent,
      failed: campaign.failed,
      errors: campaign.errors,
    };
  }

  getContacts(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    return Array.from(session.contacts.values());
  }

  getChats(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");
    const allChats = Array.from(session.chats.values());
    allChats.sort((a, b) => (b.lastMessageAt || 0) - (a.lastMessageAt || 0));
    return allChats;
  }

  getMessages(id, chatId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    let jid = chatId;
    if (!jid.includes("@")) {
      jid = this._formatJid(chatId);
    }
    const msgs = session.messages.get(jid) || [];
    return msgs.slice(-200);
  }

  markChatAsRead(id, chatId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    let jid = chatId;
    if (!jid.includes("@")) {
      jid = this._formatJid(chatId);
    }

    const chat = session.chats.get(jid);
    if (chat) {
      chat.unreadCount = 0;
    }

    return { success: true, chatId: jid };
  }

  listAll() {
    const result = [];
    for (const [, session] of this.sessions) {
      result.push({
        id: session.id,
        name: session.name,
        status: session.status,
        phone: session.phone,
        webhookUrl: session.webhookUrl || null,
      });
    }
    return result;
  }

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
