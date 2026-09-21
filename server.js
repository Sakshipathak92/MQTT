import express from "express";
import cors from "cors";
import mqtt from "mqtt";
import { WebSocketServer } from "ws";
import { ingest, allDevices, getDetails, getHistory, getStats, stats } from "./store.js";

const PORT = Number(process.env.PORT || 3000);
const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const MQTT_TOPIC = process.env.MQTT_TOPIC || "fleet/+/+/telemetry";
const STREAM_RATE_MS = Number(process.env.STREAM_RATE_MS || 1000);

const app = express();
app.use(cors());
app.use(express.json());

let mqttConnected = false;
let lastSnapshot = [];
const clients = new Set();

app.get("/health", (_req, res) => {
  res.json({ status: "ok", mqttConnected, ...getStats() });
});

app.get("/api/devices", (_req, res) => {
  res.json(allDevices());
});

app.get("/api/devices/:id", (req, res) => {
  const d = getDetails(req.params.id);
  if (!d) return res.status(404).json({ error: "device_not_found" });
  res.json(d);
});

app.get("/api/devices/:id/history", (req, res) => {
  const raw = String(req.query.window || "15m");
  const match = raw.match(/^(\d+)(s|m|h)$/);
  if (!match) return res.status(400).json({ error: "window must look like 60s, 15m or 1h" });
  const n = Number(match[1]);
  const unit = { s: 1000, m: 60_000, h: 3_600_000 }[match[2]];
  const data = getHistory(req.params.id, n * unit);
  if (!data) return res.status(404).json({ error: "device_not_found" });
  res.json(data);
});

app.get("/api/stream", (_req, res) => {
  res.status(426).json({ error: "websocket_required", endpoint: "ws://localhost:" + PORT + "/api/stream" });
});

app.get("/api/stats", (_req, res) => res.json(getStats()));

const server = app.listen(PORT, () => {
  console.log(`Ingest listening on :${PORT}`);
});
 
const wss = new WebSocketServer({ server, path: "/api/stream" });

wss.on("connection", ws => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: "snapshot", devices: allDevices() }));
  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => clients.delete(ws));
});

function broadcast() {
  const devices = allDevices();
  const payload = JSON.stringify({ type: "update", ts: new Date().toISOString(), devices });
  for (const ws of clients) {
    if (ws.readyState !== 1) continue;
    try { ws.send(payload); } catch { /* client disappears */ }
  }
}

setInterval(broadcast, STREAM_RATE_MS);

const mqttClient = mqtt.connect(MQTT_URL, {
  reconnectPeriod: 1000,
  connectTimeout: 5000,
  clean: true
});

mqttClient.on("connect", () => {
  mqttConnected = true;
  stats.reconnects++;
  mqttClient.subscribe(MQTT_TOPIC, { qos: 0 }, err => {
    if (err) console.error("MQTT subscribe failed:", err.message);
    else console.log("Subscribed:", MQTT_TOPIC);
  });
});

mqttClient.on("close", () => { mqttConnected = false; });
mqttClient.on("error", err => console.error("MQTT error:", err.message));

mqttClient.on("message", (_topic, buffer) => {
  try {
    const msg = JSON.parse(buffer.toString());
    if (!msg.deviceId || !msg.ts || !msg.state) {
      stats.dropped = (stats.dropped || 0) + 1;
      return;
    }
    ingest(msg);
  } catch {
    stats.dropped = (stats.dropped || 0) + 1;
  }
});

function shutdown(signal) {
  console.log(`${signal}: shutting down`);
  mqttClient.end(true);
  for (const ws of clients) ws.close();
  server.close(() => process.exit(0));
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
