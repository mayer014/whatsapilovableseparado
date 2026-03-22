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
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// Middleware: validate instance token
function requireInstance(req, res, next) {
  const token = req.headers["token"];
  const session = sessions.getByToken(token);
  if (!session) {
    return res.status(401).json({ error: "Invalid instance token" });
  }
  req.session = session;
  next();
}

// Health check
app.get("/health", (req, res) => {
  res.json({ status: "ok", instances: sessions.count() });
});

// Create new instance
app.post("/instance/init", requireAdmin, async (req, res) => {
  try {
    const instance = await sessions.create();
    res.json({
      id: instance.id,
      token: instance.token,
      name: instance.name,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Connect instance (generate QR)
app.post("/instance/connect", requireInstance, async (req, res) => {
  try {
    const qrcode = await sessions.connect(req.session.id);
    res.json({ qrcode });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get instance status
app.get("/instance/status", requireInstance, (req, res) => {
  const status = sessions.getStatus(req.session.id);
  res.json(status);
});

// Disconnect instance
app.post("/instance/disconnect", requireInstance, async (req, res) => {
  try {
    await sessions.disconnect(req.session.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete instance
app.delete("/instance/:id", requireAdmin, async (req, res) => {
  try {
    await sessions.remove(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Send message to single contact
app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message, type, mediaUrl } = req.body;
    const result = await sessions.sendMessage(req.session.id, {
      phone, message, type: type || "text", mediaUrl,
    });
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Bulk send (campaign)
app.post("/sender/simple", requireInstance, async (req, res) => {
  try {
    const { phones, message, mediaUrl, type, delayMin, delayMax } = req.body;
    const folderId = uuidv4();
    
    // Start sending in background
    sessions.bulkSend(req.session.id, {
      folderId,
      phones: Array.isArray(phones) ? phones : [],
      message,
      mediaUrl,
      type: type || "text",
      delayMin: delayMin || 10,
      delayMax: delayMax || 30,
    });

    res.json({ success: true, folderId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Campaign control (pause/resume/delete)
app.post("/sender/edit", requireInstance, async (req, res) => {
  try {
    const { folderId, action } = req.body;
    const result = sessions.controlCampaign(req.session.id, folderId, action);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get campaign status
app.get("/sender/status/:folderId", requireInstance, (req, res) => {
  const status = sessions.getCampaignStatus(req.session.id, req.params.folderId);
  res.json(status);
});

// List all instances (admin)
app.get("/instances", requireAdmin, (req, res) => {
  res.json(sessions.listAll());
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀 WhatsApp Engine rodando na porta ${PORT}`);
  console.log(`🔑 Admin Token: ${ADMIN_TOKEN}`);
  console.log(`\nUse este token como WHATSAPI_ADMIN_TOKEN no Lovable Cloud.\n`);
});
