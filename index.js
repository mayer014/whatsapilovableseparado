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
// MIDDLEWARE — Autenticação admin e instância
// ═══════════════════════════════════════════════════════════════

function requireAdmin(req, res, next) {
  const token = req.headers["admintoken"] || req.headers["authorization"];
  if (token !== ADMIN_TOKEN && token !== `Bearer ${ADMIN_TOKEN}`) {
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  next();
}

function requireInstance(req, res, next) {
  const token = req.headers["token"];
  const session = sessions.getByToken(token);
  if (!session) {
    return res.status(401).json({ success: false, error: "Invalid instance token" });
  }
  req.session = session;
  next();
}

// ═══════════════════════════════════════════════════════════════
// HEALTH CHECK
// ═══════════════════════════════════════════════════════════════

app.get("/health", (req, res) => {
  res.json({ success: true, status: "ok", instances: sessions.count() });
});

// ═══════════════════════════════════════════════════════════════
// MÉTRICAS REAIS DO SISTEMA (CPU, RAM, Processo Node.js)
// ═══════════════════════════════════════════════════════════════

app.get("/system/metrics", requireAdmin, (req, res) => {
  try {
    const mem = process.memoryUsage();
    const cpus = os.cpus();
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMem = totalMem - freeMem;

    // Calcular uso médio de CPU desde o boot
    let totalIdle = 0, totalTick = 0;
    cpus.forEach(cpu => {
      for (const type in cpu.times) {
        totalTick += cpu.times[type];
      }
      totalIdle += cpu.times.idle;
    });
    const cpuUsagePercent = parseFloat(((1 - totalIdle / totalTick) * 100).toFixed(1));

    res.json({
      system: {
        total_memory_mb: Math.round(totalMem / 1024 / 1024),
        used_memory_mb: Math.round(usedMem / 1024 / 1024),
        free_memory_mb: Math.round(freeMem / 1024 / 1024),
        memory_usage_percent: parseFloat(((usedMem / totalMem) * 100).toFixed(1)),
        cpu_usage_percent: cpuUsagePercent,
        cpu_cores: cpus.length,
        cpu_model: cpus[0]?.model || "unknown",
        uptime_seconds: Math.round(os.uptime()),
        process_uptime_seconds: Math.round(process.uptime()),
        load_average: os.loadavg(),
      },
      process: {
        heap_used_mb: Math.round(mem.heapUsed / 1024 / 1024),
        heap_total_mb: Math.round(mem.heapTotal / 1024 / 1024),
        rss_mb: Math.round(mem.rss / 1024 / 1024),
        external_mb: Math.round((mem.external || 0) / 1024 / 1024),
      },
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// INSTÂNCIAS — CRUD e conexão
// ═══════════════════════════════════════════════════════════════

app.post("/instance/init", requireAdmin, async (req, res) => {
  try {
    const instance = await sessions.create();
    res.json({ success: true, id: instance.id, token: instance.token, name: instance.name });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/instance/connect", requireInstance, async (req, res) => {
  try {
    const qrcode = await sessions.connect(req.session.id);
    res.json({ success: true, qrcode });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/instance/status", requireInstance, (req, res) => {
  try {
    const status = sessions.getStatus(req.session.id);
    res.json({ success: true, ...status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/instance/disconnect", requireInstance, async (req, res) => {
  try {
    await sessions.disconnect(req.session.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.delete("/instance/:id", requireAdmin, async (req, res) => {
  try {
    await sessions.remove(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// WEBHOOK — Configurar URL de callback por instância
// ═══════════════════════════════════════════════════════════════

app.post("/instance/setWebhook", requireInstance, (req, res) => {
  try {
    const { webhookUrl } = req.body;
    const result = sessions.setWebhook(req.session.id, webhookUrl);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/instance/webhook", requireInstance, (req, res) => {
  try {
    const result = sessions.getWebhook(req.session.id);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// MENSAGENS — Envio individual e em massa
// ═══════════════════════════════════════════════════════════════

app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message, type, mediaUrl } = req.body;
    if (!phone) return res.status(400).json({ success: false, error: "phone is required" });
    if (!message && !mediaUrl) return res.status(400).json({ success: false, error: "message or mediaUrl is required" });
    const result = await sessions.sendMessage(req.session.id, { phone, message, type: type || "text", mediaUrl });
    res.json({ success: true, delivered: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/sender/simple", requireInstance, async (req, res) => {
  try {
    const { phones, message, mediaUrl, type, delayMin, delayMax } = req.body;
    if (!Array.isArray(phones) || phones.length === 0) {
      return res.status(400).json({ success: false, error: "phones must be a non-empty array" });
    }
    const folderId = uuidv4();
    sessions.bulkSend(req.session.id, {
      folderId, phones, message, mediaUrl,
      type: type || "text", delayMin: delayMin || 10, delayMax: delayMax || 30,
    });
    res.json({ success: true, folderId });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/sender/edit", requireInstance, async (req, res) => {
  try {
    const { folderId, action } = req.body;
    const result = sessions.controlCampaign(req.session.id, folderId, action);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/sender/status/:folderId", requireInstance, (req, res) => {
  try {
    const status = sessions.getCampaignStatus(req.session.id, req.params.folderId);
    res.json({ success: true, ...status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// LISTAGENS — Instâncias, contatos, chats, mensagens
// ═══════════════════════════════════════════════════════════════

app.get("/instances", requireAdmin, (req, res) => {
  try {
    res.json({ success: true, instances: sessions.listAll() });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/contacts", requireInstance, (req, res) => {
  try {
    res.json({ success: true, contacts: sessions.getContacts(req.session.id) });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/chats", requireInstance, (req, res) => {
  try {
    res.json({ success: true, chats: sessions.getChats(req.session.id) });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/messages/:chatId", requireInstance, (req, res) => {
  try {
    const chatId = decodeURIComponent(req.params.chatId);
    const messages = sessions.getMessages(req.session.id, chatId);
    res.json({ success: true, messages });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/messages", requireInstance, (req, res) => {
  try {
    const { chatId } = req.query;
    if (!chatId) return res.status(400).json({ success: false, error: "chatId is required" });
    const messages = sessions.getMessages(req.session.id, String(chatId));
    res.json({ success: true, messages });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/chat/read", requireInstance, (req, res) => {
  try {
    const { chatId } = req.body;
    if (!chatId) return res.status(400).json({ success: false, error: "chatId is required" });
    const result = sessions.markChatAsRead(req.session.id, chatId);
    res.json(result);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// ROTA RAIZ — Informações do serviço
// ═══════════════════════════════════════════════════════════════

app.get("/", (req, res) => {
  res.json({
    success: true,
    service: "WhatsApp Engine",
    status: "online",
    version: "1.3.0",
    routes: [
      "GET /health",
      "GET /system/metrics",
      "POST /instance/init",
      "POST /instance/connect",
      "GET /instance/status",
      "POST /instance/disconnect",
      "DELETE /instance/:id",
      "POST /instance/setWebhook",
      "GET /instance/webhook",
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

// ═══════════════════════════════════════════════════════════════
// INICIAR SERVIDOR
// ═══════════════════════════════════════════════════════════════

app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀 WhatsApp Engine v1.3.0 rodando na porta ${PORT}`);
  console.log(`🔑 Admin Token: ${ADMIN_TOKEN}`);
  console.log(`📡 Webhook support: ENABLED`);
  console.log(`📊 System metrics: GET /system/metrics`);
  console.log(`\nUse este token como WHATSAPI_ADMIN_TOKEN no Lovable Cloud.\n`);
});
