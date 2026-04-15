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
// MÉTRICAS DO SISTEMA
// ═══════════════════════════════════════════════════════════════

app.get("/system/metrics", requireAdmin, (req, res) => {
  try {
    const mem = process.memoryUsage();
    const cpus = os.cpus();
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMem = totalMem - freeMem;

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
// INSTÂNCIAS
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
// WEBHOOK
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
// MENSAGENS
// ═══════════════════════════════════════════════════════════════

app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message, type, mediaUrl } = req.body;
    if (!phone) return res.status(400).json({ success: false, error: "phone is required" });
    if (!message && !mediaUrl) return res.status(400).json({ success: false, error: "message or mediaUrl is required" });

    const result = await sessions.sendMessage(req.session.id, {
      phone,
      message,
      type: type || "text",
      mediaUrl
    });

    res.json({ success: true, delivered: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// MÍDIA (🔥 NOVO)
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
        error: result.error,
      });
    }

    res.set("Content-Type", result.mimetype);
    res.set("Content-Disposition", "inline");
    res.set("Cache-Control", "public, max-age=3600");

    res.send(result.buffer);
  } catch (err) {
    res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});

// ═══════════════════════════════════════════════════════════════
// ROOT
// ═══════════════════════════════════════════════════════════════

app.get("/", (req, res) => {
  res.json({
    success: true,
    service: "WhatsApp Engine",
    status: "online",
    version: "1.4.0",
  });
});

// ═══════════════════════════════════════════════════════════════
// START
// ═══════════════════════════════════════════════════════════════

app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 WhatsApp Engine rodando na porta ${PORT}`);
  console.log(`🔑 Admin Token: ${ADMIN_TOKEN}`);
});
