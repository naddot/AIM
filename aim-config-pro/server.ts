import express from "express";
import { GoogleAuth } from "google-auth-library";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import cors from "cors";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 8080;

// ----------------------------------------------------------------------
// Production Server Configuration
// This server acts as the Frontend Host and API Gateway.
// ----------------------------------------------------------------------

console.log("\n================================================");
console.log("🚀 SERVER STARTING");
console.log(`📅 Timestamp: ${new Date().toISOString()}`);
console.log(`📂 Working Dir: ${process.cwd()}`);
console.log("================================================\n");

// Middleware
app.use(cors() as any);
app.use(express.json() as any);

// Request Logging
app.use((req, res, next) => {
  console.log(`[REQUEST] ${req.method} ${req.url}`);
  next();
});

// Configuration
const CONFIG_SERVICE_URL = "https://update-aim-config-829092209663.europe-west1.run.app";
const SECRET_NAME = "projects/829092209663/secrets/AIM-config-growth-update/versions/latest";

// ----------------------------------------------------------------------
// API Routes
// ----------------------------------------------------------------------

app.get("/api/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Primary Config Save Endpoint
app.post("/api/save-config", async (req, res) => {
  console.log(`\n[${new Date().toISOString()}] 📥 Processing Configuration Update`);

  try {
    const payload = req.body || {};

    // 1. Get Credentials from Secret Manager
    console.log("🔐 Fetching credentials from Secret Manager...");
    const secretClient = new SecretManagerServiceClient();
    const [version] = await secretClient.accessSecretVersion({
      name: SECRET_NAME,
    }).catch((err: any) => {
      console.error("❌ Secret Manager Access Failed:", err.message);
      throw new Error(`Failed to access Secret Manager: ${err.message}`);
    });

    const secretPayload = version.payload?.data?.toString();
    if (!secretPayload) throw new Error("Secret payload is empty");
    const credentials = JSON.parse(secretPayload.trim());
    console.log("✅ Credentials loaded.");

    // 2. Authenticate
    console.log("🔑 Authenticating with Google Cloud...");
    const auth = new GoogleAuth({
      credentials,
    });

    const client = await auth.getIdTokenClient(CONFIG_SERVICE_URL);

    // 3. Forward to Backend Service
    console.log(`➡ Forwarding to Backend: ${CONFIG_SERVICE_URL}`);
    const targetResponse = await client.request({
      url: CONFIG_SERVICE_URL,
      method: "POST",
      data: payload,
      headers: { "Content-Type": "application/json" },
      timeout: 15000,
    });

    console.log(`✅ Backend Response: ${targetResponse.status}`);
    res.status(targetResponse.status).send(targetResponse.data);

  } catch (err: any) {
    console.error("❌ API Error:", err.message);
    const status = err.response?.status || 500;
    const details = err.response?.data || err.message;
    res.status(status).send({ error: "Configuration Update Failed", details });
  }
});

// ----------------------------------------------------------------------
// Static Files & Frontend Routing
// ----------------------------------------------------------------------

const STATIC_PATH = path.join(__dirname, "dist");

console.log(`📂 Checking static path: ${STATIC_PATH}`);
if (fs.existsSync(STATIC_PATH)) {
  console.log("✅ Static 'dist' folder found. Serving React app.");
  app.use(express.static(STATIC_PATH) as any);

  // SPA Catch-all: Verify file exists before serving index.html to avoid loops
  app.get("*", (req, res) => {
    if (req.path.startsWith('/api')) {
      // Don't serve HTML for missing API routes
      res.status(404).json({ error: "API Route Not Found" });
      return;
    }
    res.sendFile(path.join(STATIC_PATH, "index.html"));
  });
} else {
  console.warn("⚠️ 'dist' folder NOT found. Run 'npm run build' before starting the server.");
  app.get("*", (req, res) => {
    res.status(500).send("Server Error: Static content not found.");
  });
}

// Debug: Print registered routes
setTimeout(() => {
  console.log("\n📋 Registered Routes:");
  (app as any)._router.stack.forEach((r: any) => {
    if (r.route && r.route.path) {
      console.log(`   ${Object.keys(r.route.methods).join(', ').toUpperCase()} ${r.route.path}`);
    }
  });
  console.log("================================================\n");
}, 100);

app.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});