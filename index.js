import mqtt from "mqtt";

const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const TOPIC_TEMPLATE = process.env.MQTT_TOPIC_TEMPLATE || "fleet/{regionId}/{deviceId}/telemetry";
const N = Number(process.env.DEVICE_COUNT || 20);
const HZ = Number(process.env.PUBLISH_HZ || 10);

const rates = {
  duplicate: Number(process.env.DUPLICATE_RATE || 0.01),
  late: Number(process.env.LATE_RATE || 0.02),
  reset: Number(process.env.RESET_RATE || 0.0007),
  silent: Number(process.env.SILENT_DEVICE_RATE || 0.05)
};

const silentMin = Number(process.env.SILENT_MIN_MS || 30_000);
const silentMax = Number(process.env.SILENT_MAX_MS || 60_000);

const states = ["MOVING", "IDLE", "CHARGING", "OFF"];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function rand(min, max) { return min + Math.random() * (max - min); }
function choose(a) { return a[Math.floor(Math.random() * a.length)]; }

function topic(device) {
  return TOPIC_TEMPLATE
    .replace("{regionId}", device.regionId)
    .replace("{deviceId}", device.deviceId);
}

function createDevice(i) {
  return {
    regionId: "west-01",
    deviceId: `VEH-${String(1042 + i).padStart(4, "0")}`,
    state: choose(["MOVING", "IDLE"]),
    speedKph: 0,
    batteryPct: rand(45, 95),
    odometerMeters: rand(10_000, 150_000),
    routeId: `R-${String(118 + (i % 8)).padStart(3, "0")}`,
    faults: [],
    silentUntil: 0,
    previous: null
  };
}

function transition(device) {
  if (Math.random() > 0.015) return;
  const next = {
    MOVING: ["MOVING", "IDLE"],
    IDLE: ["IDLE", "MOVING", "CHARGING"],
    CHARGING: ["CHARGING", "IDLE"],
    OFF: ["OFF", "IDLE"]
  }[device.state];
  device.state = choose(next);
}

function buildReading(device) {
  transition(device);

  if (device.state === "MOVING") {
    device.speedKph = Math.max(5, Math.min(80, device.speedKph + rand(-5, 5)));
    device.odometerMeters += (device.speedKph / 3.6) / HZ;
    device.batteryPct = Math.max(0, device.batteryPct - 0.0008);
  } else if (device.state === "CHARGING") {
    device.speedKph = 0;
    device.batteryPct = Math.min(100, device.batteryPct + 0.01);
  } else {
    device.speedKph = 0;
  }

  const faults = [];
  if (Math.random() < 0.01) faults.push("LOW_TYRE_PRESSURE");

  const msg = {
    deviceId: device.deviceId,
    ts: new Date().toISOString(),
    state: device.state,
    speedKph: Number(device.speedKph.toFixed(1)),
    batteryPct: Number(device.batteryPct.toFixed(1)),
    odometerMeters: Math.floor(device.odometerMeters),
    routeId: device.routeId,
    faults
  };

  return msg;
}

async function runDevice(client, device) {
  const period = 1000 / HZ;

  while (!shuttingDown) {
    const started = Date.now();

    if (Date.now() < device.silentUntil) {
      await sleep(Math.min(period, device.silentUntil - Date.now()));
      continue;
    }

    if (Math.random() < rates.silent) {
      device.silentUntil = Date.now() + rand(silentMin, silentMax);
      continue;
    }

    if (Math.random() < rates.reset) {
      device.odometerMeters = 0;
      console.log(`${device.deviceId}: injected counter reset`);
    }

    const msg = buildReading(device);

    if (Math.random() < rates.late && device.previous) {
      const late = { ...device.previous };
      // Keep original device timestamp; delay publication by up to ~1 sec.
      setTimeout(() => client.publish(topic(device), JSON.stringify(late)), rand(100, 1000));
    }

    const payload = JSON.stringify(msg);
    client.publish(topic(device), payload);

    if (Math.random() < rates.duplicate) {
      client.publish(topic(device), payload);
    }

    device.previous = msg;

    const elapsed = Date.now() - started;
    await sleep(Math.max(0, period - elapsed));
  }
}

let shuttingDown = false;

const client = mqtt.connect(MQTT_URL, {
  reconnectPeriod: 1000,
  connectTimeout: 5000,
  clean: true
});

client.on("connect", async () => {
  console.log(`Simulator connected to ${MQTT_URL}`);
  const devices = Array.from({ length: N }, (_, i) => createDevice(i));
  await Promise.all(devices.map(d => runDevice(client, d)));
});

client.on("error", err => console.error("Simulator MQTT error:", err.message));
client.on("reconnect", () => console.log("Simulator reconnecting..."));

function shutdown() {
  shuttingDown = true;
  client.end(true, () => process.exit(0));
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
