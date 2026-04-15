const express = require("express");
const cors = require("cors");
const os = require("os");
const { v4: uuidv4 } = require("uuid");
const { SessionManager } = require("./session-manager");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || uuidv4();

const sessions = new SessionManager();

// ═══════════════════════════════════════════════════════════════
// 🔐 MIDDLEWARES
// ═══════════════════════════════════════════════════════════════

function requireAdmin(req, res, next) {
  const token = req.headers["admintoken"] || req.headers["authorization"];

  if (token !== ADMIN_TOKEN && token !== `Bearer ${ADMIN_TOKEN}`) {
    return res.status(401).json({
      success: false,
      error: "Unauthorized",
    });
  }

  next();
}

function requireInstance(req, res, next) {
  const token = req.headers["token"];
  const session = sessions.getByToken(token);

  if (!session) {
    return res.status(401).json({
      success: false,
      error: "Invalid instance token",
    });
  }

  req.session = session;
  next();
}

// ═══════════════════════════════════════════════════════════════
// ❤️ HEALTH
// ═══════════════════════════════════════════════════════════════

app.get("/health", (req, res) => {
  res.json({
    success: true,
    status: "ok",
    instances: sessions.sessions.size,
  });
});

// ═══════════════════════════════════════════════════════════════
// 📦 INSTÂNCIAS
// ═══════════════════════════════════════════════════════════════

app.post("/instance/init", requireAdmin, async (req, res) => {
  try {
    const instance = await sessions.create();

    return res.json({
      success: true,
      id: instance.id,
      token: instance.token,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});

app.post("/instance/connect", requireInstance, async (req, res) => {
  try {
    const qrcode = await sessions.connect(req.session.id);

    return res.json({
      success: true,
      qrcode,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});

app.get("/instance/status", requireInstance, (req, res) => {
  try {
    const status = sessions.getStatus(req.session.id);

    return res.json({
      success: true,
      ...status,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});

// ═══════════════════════════════════════════════════════════════
// 💬 ENVIO DE MENSAGEM (FIX REAL)
// ═══════════════════════════════════════════════════════════════

app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message } = req.body;

    if (!phone || !message) {
      return res.status(400).json({
        success: false,
        error: "phone and message are required",
      });
    }

    const result = await sessions.sendMessage(req.session.id, {
      phone,
      message,
    });

    // 🔥 GARANTIR JSON SEMPRE
    return res.json({
      success: true,
      delivered: true,
      messageId: result?.messageId || null,
      to: result?.to || null,
    });

  } catch (err) {
    console.error("❌ SEND ERROR:", err);

    return res.status(500).json({
      success: false,
      error: err.message || "send failed",
    });
  }
});

// ═══════════════════════════════════════════════════════════════
// 🖼️ MÍDIA
// ═══════════════════════════════════════════════════════════════

app.get("/media/:messageId", requireInstance, async (req, res) => {
  try {
    const result = await sessions.downloadMedia(
      req.session.id,
      req.params.messageId
    );

    if (!result.found) {
      return res.status(410).json({
        success: false,
        error: "Media not found",
      });
    }

    res.set("Content-Type", result.mimetype || "application/octet-stream");
    res.set("Content-Disposition", "inline");

    return res.send(result.buffer);
  } catch (err) {
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});

// ═══════════════════════════════════════════════════════════════
// 🏠 ROOT
// ═══════════════════════════════════════════════════════════════

app.get("/", (req, res) => {
  res.json({
    success: true,
    service: "WhatsApp Engine",
    status: "online",
    version: "1.6.0",
  });
});

// ═══════════════════════════════════════════════════════════════
// 🚀 START
// ═══════════════════════════════════════════════════════════════

app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 WhatsApp Engine rodando na porta ${PORT}`);
  console.log(`🔑 Admin Token: ${ADMIN_TOKEN}`);
});
