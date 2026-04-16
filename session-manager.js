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
const axios = require("axios");

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
      webhook: null,
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
      markOnlineOnConnect: true,
      printQRInTerminal: false,
    });

    session.socket = socket;
    session.status = "connecting";

    socket.ev.on("creds.update", saveCreds);

    // 🔥 RECEBIMENTO + WEBHOOK
    socket.ev.on("messages.upsert", async ({ messages }) => {
      for (const msg of messages) {
        if (!msg.message) continue;

        const jid = msg.key.remoteJid;
        if (!jid || jid === "status@broadcast") continue;

        const messageId = msg.key.id;

        const text =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          "";

        // Guardar no índice para download de mídia
        session.messageIndex.set(messageId, {
          key: msg.key,
          message: msg.message,
        });

        console.log("📩 Nova mensagem:", text);

        // 🔥 WEBHOOK (ESSENCIAL) — Envia tanto mensagens recebidas quanto enviadas
        if (session.webhook) {
          try {
            await axios.post(session.webhook, {
              event: "message",
              instanceId: session.id,
              from: jid,
              fromMe: msg.key.fromMe || false,
              messageId,
              text,
            });
          } catch (err) {
            console.log("⚠️ Falha webhook:", err.message);
          }
        }
      }
    });

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
          console.log("🔌 Desconectado (loggedOut):", id);
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
        // Se já conectou direto (sessão salva), retorna status
        if (session.status === "connected") {
          clearInterval(timer);
          clearTimeout(timeout);
          resolve(null);
        }
      }, 500);
    });
  }

  /**
   * Desconecta uma instância e limpa a sessão.
   */
  async disconnect(id) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    if (session.socket) {
      try {
        await session.socket.logout();
      } catch (err) {
        console.log("⚠️ Erro ao fazer logout:", err.message);
        // Tenta fechar a conexão de qualquer forma
        try {
          session.socket.end();
        } catch (_) {}
      }
    }

    session.socket = null;
    session.status = "disconnected";
    session.qrcode = null;
    session.phone = null;

    // Limpa os arquivos de sessão para forçar novo QR no próximo connect
    const sessionDir = path.join(SESSIONS_DIR, id);
    if (fs.existsSync(sessionDir)) {
      fs.rmSync(sessionDir, { recursive: true, force: true });
    }

    console.log("🔌 Instância desconectada manualmente:", id);
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

  /**
   * Envia mensagem de texto validando o número no WhatsApp antes.
   *
   * CORREÇÃO PRINCIPAL:
   * 1. Usa onWhatsApp() para verificar se o número existe
   * 2. Usa o JID correto retornado pelo WhatsApp (resolve o problema do 9º dígito brasileiro)
   * 3. Retorna delivered baseado no resultado real do envio
   */
  async sendMessage(id, { phone, message }) {
    const session = this.sessions.get(id);

    if (!session?.socket) {
      throw new Error("Instance not connected");
    }

    // Limpa o número removendo tudo que não é dígito
    const clean = String(phone).replace(/\D/g, "");

    if (!clean || clean.length < 10) {
      throw new Error("Número de telefone inválido: " + phone);
    }

    // ═══════════════════════════════════════════════════════════════
    // VALIDAÇÃO NO WHATSAPP — Resolve o problema do 9º dígito
    // O WhatsApp retorna o JID correto independente do formato enviado
    // ═══════════════════════════════════════════════════════════════
    let targetJid;

    try {
      const [result] = await session.socket.onWhatsApp(clean);

      if (!result || !result.exists) {
        // Tenta com/sem o 9º dígito para DDDs brasileiros
        const alternative = tryBrazilianAlternative(clean);

        if (alternative) {
          const [altResult] = await session.socket.onWhatsApp(alternative);

          if (!altResult || !altResult.exists) {
            throw new Error(
              `Número ${clean} não encontrado no WhatsApp (tentou também ${alternative})`
            );
          }

          targetJid = altResult.jid;
          console.log(`📱 Número corrigido: ${clean} → ${alternative} (JID: ${targetJid})`);
        } else {
          throw new Error(`Número ${clean} não encontrado no WhatsApp`);
        }
      } else {
        targetJid = result.jid;
        console.log(`📱 Número validado: ${clean} (JID: ${targetJid})`);
      }
    } catch (err) {
      // Se onWhatsApp falhar (ex: timeout), tenta enviar direto
      if (err.message.includes("não encontrado")) {
        throw err;
      }
      console.log("⚠️ onWhatsApp falhou, tentando envio direto:", err.message);
      targetJid = `${clean}@s.whatsapp.net`;
    }

    // ═══════════════════════════════════════════════════════════════
    // ENVIO DA MENSAGEM
    // ═══════════════════════════════════════════════════════════════
    const sent = await session.socket.sendMessage(targetJid, { text: message });

    const delivered = !!(sent?.key?.id);

    console.log(
      delivered
        ? `✅ Mensagem enviada para ${targetJid} (ID: ${sent.key.id})`
        : `❌ Falha ao enviar para ${targetJid}`
    );

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
    return { webhook: url };
  }

  async downloadMedia(id, messageId) {
    const session = this.sessions.get(id);
    if (!session) throw new Error("Session not found");

    const rawMsg = session.messageIndex.get(messageId);

    if (!rawMsg) return { found: false };

    try {
      const buffer = await downloadMediaMessage(rawMsg, "buffer", {});
      return {
        found: true,
        buffer,
        mimetype: rawMsg.message?.imageMessage?.mimetype ||
                  rawMsg.message?.videoMessage?.mimetype ||
                  rawMsg.message?.audioMessage?.mimetype ||
                  rawMsg.message?.documentMessage?.mimetype ||
                  "application/octet-stream",
      };
    } catch (err) {
      console.log("⚠️ Erro ao baixar mídia:", err.message);
      return { found: false };
    }
  }
}

/**
 * Para números brasileiros, tenta a variante com/sem o 9º dígito.
 *
 * Formato BR: 55 + DDD(2) + número(8 ou 9)
 * - Com 9: 5567992248348 (13 dígitos)
 * - Sem 9: 556792248348  (12 dígitos)
 *
 * O WhatsApp pode registrar o número em qualquer um dos formatos.
 */
function tryBrazilianAlternative(phone) {
  if (!phone.startsWith("55")) return null;

  const ddd = phone.substring(2, 4);
  const rest = phone.substring(4);

  // Se tem 9 dígitos após DDD (com o 9), tenta sem
  if (rest.length === 9 && rest.startsWith("9")) {
    return "55" + ddd + rest.substring(1);
  }

  // Se tem 8 dígitos após DDD (sem o 9), tenta com
  if (rest.length === 8) {
    return "55" + ddd + "9" + rest;
  }

  return null;
}

module.exports = { SessionManager };
