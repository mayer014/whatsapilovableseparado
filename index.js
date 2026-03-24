const express = require("express");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const { SessionManager } = require("./session-manager");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || uuidv4();

const sessions = new SessionManager();

// Middleware: validate admin token
function requireAdmin(req, res, next) {
  const token = req.headers["admintoken"] || req.headers["authorization"];
  if (token !== ADMIN_TOKEN && token !== `Bearer ${ADMIN_TOKEN}`) {
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  next();
}

// Middleware: validate instance token
function requireInstance(req, res, next) {
  const token = req.headers["token"];
  const session = sessions.getByToken(token);
  if (!session) {
    return res.status(401).json({ success: false, error: "Invalid instance token" });
  }
  req.session = session;
  next();
}

// Health check
app.get("/health", (req, res) => {
  res.json({
    success: true,
    status: "ok",
    instances: sessions.count(),
  });
});

// Create new instance
app.post("/instance/init", requireAdmin, async (req, res) => {
  try {
    const instance = await sessions.create();
    res.json({
      success: true,
      id: instance.id,
      token: instance.token,
      name: instance.name,
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Connect instance (generate QR)
app.post("/instance/connect", requireInstance, async (req, res) => {
  try {
    const qrcode = await sessions.connect(req.session.id);
    res.json({ success: true, qrcode });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get instance status
app.get("/instance/status", requireInstance, (req, res) => {
  try {
    const status = sessions.getStatus(req.session.id);
    res.json({ success: true, ...status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Disconnect instance
app.post("/instance/disconnect", requireInstance, async (req, res) => {
  try {
    await sessions.disconnect(req.session.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Delete instance
app.delete("/instance/:id", requireAdmin, async (req, res) => {
  try {
    await sessions.remove(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Send message to single contact
app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message, type, mediaUrl } = req.body;

    if (!phone) {
      return res.status(400).json({ success: false, error: "phone is required" });
    }

    if (!message && !mediaUrl) {
      return res.status(400).json({ success: false, error: "message or mediaUrl is required" });
    }

    const result = await sessions.sendMessage(req.session.id, {
      phone,
      message,
      type: type || "text",
      mediaUrl,
    });

    res.json({ success: true, delivered: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Bulk send (campaign)
app.post("/sender/simple", requireInstance, async (req, res) => {
  try {
    const { phones, message, mediaUrl, type, delayMin, delayMax } = req.body;

    if (!Array.isArray(phones) || phones.length === 0) {
      return res.status(400).json({
        success: false,
        error: "phones must be a non-empty array",
      });
    }

    const folderId = uuidv4();

    sessions.bulkSend(req.session.id, {
      folderId,
      phones,
      message,
      mediaUrl,
      type: type || "text",
      delayMin: delayMin || 10,
      delayMax: delayMax || 30,
    });

    res.json({ success: true, folderId });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Campaign control (pause/resume/delete)
app.post("/sender/edit", requireInstance, async (req, res) => {
  try {
    const { folderId, action } = req.body;
    const result = sessions.controlCampaign(req.session.id, folderId, action);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get campaign status
app.get("/sender/status/:folderId", requireInstance, (req, res) => {
  try {
    const status = sessions.getCampaignStatus(req.session.id, req.params.folderId);
    res.json({ success: true, ...status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// List all instances (admin)
app.get("/instances", requireAdmin, (req, res) => {
  try {
    res.json({ success: true, instances: sessions.listAll() });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// =========================
// NOVAS ROTAS DE LEITURA
// =========================

// List contacts for current instance
app.get("/contacts", requireInstance, (req, res) => {
  try {
    const contacts = sessions.getContacts(req.session.id);
    res.json({ success: true, contacts });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// List chats/conversations for current instance
app.get("/chats", requireInstance, (req, res) => {
  try {
    const chats = sessions.getChats(req.session.id);
    res.json({ success: true, chats });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get messages from one chat
app.get("/messages/:chatId", requireInstance, (req, res) => {
  try {
    const chatId = decodeURIComponent(req.params.chatId);
    const messages = sessions.getMessages(req.session.id, chatId);
    res.json({ success: true, messages });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Optional: all messages for a chat via querystring
app.get("/messages", requireInstance, (req, res) => {
  try {
    const { chatId } = req.query;

    if (!chatId) {
      return res.status(400).json({ success: false, error: "chatId is required" });
    }

    const messages = sessions.getMessages(req.session.id, String(chatId));
    res.json({ success: true, messages });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Mark chat as read
app.post("/chat/read", requireInstance, (req, res) => {
  try {
    const { chatId } = req.body;

    if (!chatId) {
      return res.status(400).json({ success: false, error: "chatId is required" });
    }

    const result = sessions.markChatAsRead(req.session.id, chatId);
    res.json(result);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Home route
app.get("/", (req, res) => {
  res.json({
    success: true,
    service: "WhatsApp Engine",
    status: "online",
    version: "1.1.0",
    routes: [
      "GET /health",
      "POST /instance/init",
      "POST /instance/connect",
      "GET /instance/status",
      "POST /instance/disconnect",
      "DELETE /instance/:id",
      "POST /message/send",
      "POST /sender/simple",
      "POST /sender/edit",
      "GET /sender/status/:folderId",
      "GET /instances",
      "GET /contacts",
      "GET /chats",
      "GET /messages/:chatId",
      "GET /messages?chatId=...",
      "POST /chat/read",
    ],
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀 WhatsApp Engine rodando na porta ${PORT}`);
  console.log(`🔑 Admin Token: ${ADMIN_TOKEN}`);
  console.log(`\nUse este token como WHATSAPI_ADMIN_TOKEN no Lovable Cloud.\n`);
});
