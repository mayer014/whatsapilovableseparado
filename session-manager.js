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

// 🔐 AUTH
function requireAdmin(req, res, next) {
  const token = req.headers["admintoken"];
  if (token !== ADMIN_TOKEN) {
    return res.status(401).json({ success: false });
  }
  next();
}

function requireInstance(req, res, next) {
  const token = req.headers["token"];
  const session = sessions.getByToken(token);

  if (!session) {
    return res.status(401).json({ success: false });
  }

  req.session = session;
  next();
}

// ❤️ HEALTH
app.get("/health", (req, res) => {
  res.json({
    success: true,
    instances: sessions.sessions.size,
  });
});

// 📦 INSTÂNCIAS
app.post("/instance/init", requireAdmin, async (req, res) => {
  const instance = await sessions.create();
  res.json({ success: true, ...instance });
});

app.post("/instance/connect", requireInstance, async (req, res) => {
  const qr = await sessions.connect(req.session.id);
  res.json({ success: true, qrcode: qr });
});

app.get("/instance/status", requireInstance, (req, res) => {
  res.json({
    success: true,
    ...sessions.getStatus(req.session.id),
  });
});

// 💬 ENVIO
app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message } = req.body;

    const result = await sessions.sendMessage(req.session.id, {
      phone,
      message,
    });

    res.json({
      success: true,
      delivered: true,
      ...result,
    });
  } catch (err) {
    res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});

// 🔥 WEBHOOK
app.post("/instance/setWebhook", requireInstance, (req, res) => {
  const { webhookUrl } = req.body;

  const result = sessions.setWebhook(req.session.id, webhookUrl);

  res.json({
    success: true,
    ...result,
  });
});

// 🖼️ MEDIA
app.get("/media/:messageId", requireInstance, async (req, res) => {
  const result = await sessions.downloadMedia(
    req.session.id,
    req.params.messageId
  );

  if (!result.found) {
    return res.status(404).json({ success: false });
  }

  res.set("Content-Type", result.mimetype);
  res.send(result.buffer);
});

// 🚀 START
app.listen(PORT, "0.0.0.0", () => {
  console.log("🚀 WhatsApp Engine rodando");
  console.log("🔑 Token:", ADMIN_TOKEN);
});
