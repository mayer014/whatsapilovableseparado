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
    return res.status(401).json({ success: false, error: "Invalid admin token" });
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
  try {
    const qr = await sessions.connect(req.session.id);
    res.json({ success: true, qrcode: qr });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/instance/status", requireInstance, (req, res) => {
  res.json({
    success: true,
    ...sessions.getStatus(req.session.id),
  });
});

app.post("/instance/disconnect", requireInstance, async (req, res) => {
  try {
    await sessions.disconnect(req.session.id);
    res.json({ success: true, message: "Instância desconectada." });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 💬 ENVIO — Rota principal usada pela Edge Function
app.post("/sender/simple", requireInstance, async (req, res) => {
  try {
    const { phones, message, delayMin, delayMax } = req.body;

    if (!phones || !Array.isArray(phones) || phones.length === 0 || !message) {
      return res.status(400).json({ success: false, error: "Missing phones array or message" });
    }

    const results = [];

    for (const phone of phones) {
      try {
        const result = await sessions.sendMessage(req.session.id, { phone, message });
        results.push({ phone, ...result });
      } catch (err) {
        results.push({ phone, success: false, error: err.message });
      }

      // Delay aleatório entre envios (se houver mais de um número)
      if (phones.length > 1 && delayMin && delayMax) {
        const delay = Math.floor(Math.random() * (delayMax - delayMin + 1) + delayMin) * 1000;
        await new Promise((r) => setTimeout(r, delay));
      }
    }

    // Se enviou para apenas um número, retorna resultado direto
    if (results.length === 1) {
      return res.json(results[0]);
    }

    res.json({ success: true, results });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 💬 ENVIO — Rota legada (fallback)
app.post("/message/send", requireInstance, async (req, res) => {
  try {
    const { phone, message } = req.body;

    if (!phone || !message) {
      return res.status(400).json({ success: false, error: "Missing phone or message" });
    }

    const result = await sessions.sendMessage(req.session.id, { phone, message });
    res.json(result);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 🔥 WEBHOOK
app.post("/instance/setWebhook", requireInstance, (req, res) => {
  const { webhookUrl } = req.body;
  const result = sessions.setWebhook(req.session.id, webhookUrl);
  res.json({ success: true, ...result });
});

// 🖼️ MEDIA
app.get("/media/:messageId", requireInstance, async (req, res) => {
  try {
    const result = await sessions.downloadMedia(req.session.id, req.params.messageId);

    if (!result.found) {
      return res.status(404).json({ success: false, error: "Media not found" });
    }

    res.set("Content-Type", result.mimetype);
    res.send(result.buffer);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 📊 MÉTRICAS DO SISTEMA
app.get("/system/metrics", requireAdmin, (req, res) => {
  const memUsage = process.memoryUsage();
  const totalMem = require("os").totalmem();
  const freeMem = require("os").freemem();
  const usedMem = totalMem - freeMem;
  const cpus = require("os").cpus();
  const uptime = require("os").uptime();

  res.json({
    success: true,
    system: {
      total_memory_mb: Math.round(totalMem / 1024 / 1024),
      used_memory_mb: Math.round(usedMem / 1024 / 1024),
      free_memory_mb: Math.round(freeMem / 1024 / 1024),
      memory_usage_percent: ((usedMem / totalMem) * 100).toFixed(1),
      cpu_cores: cpus.length,
      cpu_model: cpus[0]?.model || "unknown",
      cpu_usage_percent: "0",
      uptime_seconds: uptime,
      process_uptime_seconds: process.uptime(),
      load_average: require("os").loadavg(),
    },
    process: {
      heap_used_mb: Math.round(memUsage.heapUsed / 1024 / 1024),
      heap_total_mb: Math.round(memUsage.heapTotal / 1024 / 1024),
      rss_mb: Math.round(memUsage.rss / 1024 / 1024),
    },
  });
});

// 📋 LISTAR INSTÂNCIAS ATIVAS
app.get("/instances", requireAdmin, (req, res) => {
  const list = [];
  for (const [id, session] of sessions.sessions) {
    list.push({
      id,
      token: session.token,
      status: session.status,
      phone: session.phone,
    });
  }
  res.json(list);
});

// 🚀 START
app.listen(PORT, "0.0.0.0", () => {
  console.log("🚀 WhatsApp Engine rodando na porta", PORT);
  console.log("🔑 Token:", ADMIN_TOKEN);
});
