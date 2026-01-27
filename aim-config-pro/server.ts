import "dotenv/config"; // Load environment variables first
import express from "express";
import { GoogleAuth } from "google-auth-library";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import cors from "cors";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import { Storage } from "@google-cloud/storage";
import { getActiveExecution, triggerJob } from "./services/cloudRunJobs.js"; // Note: .js extension for ES modules if needed, or tsx handles resolution

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
console.log(`🌍 Env Check: PROJECT_ID=${process.env.PROJECT_ID}, REGION=${process.env.REGION}, JOB_NAME=${process.env.JOB_NAME}`);
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

// Check Job Status
app.get("/api/job-status", async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  try {
    const since = req.query.since as string | undefined;
    const status = await getActiveExecution(since);
    res.json(status);
  } catch (err: any) {
    console.error("[/api/job-status] ERROR:", err?.stack || err);
    // Don't just return 500, try to return a meaningful error that the UI can withstand
    res.status(500).json({ error: "Job status check failed", details: err?.message || String(err) });
  }
});

// Fetch Current Config from GCS
app.get("/api/current-config", async (req, res) => {
  console.log(`\n[${new Date().toISOString()}] 📥 Fetching Current Configuration from GCS`);

  try {
    const gcsUri = process.env.CONFIG_GCS_URI || "gs://aim-home/aim-config-files/aim-config.json";
    console.log(`Target GCS URI: ${gcsUri}`);

    if (!gcsUri.startsWith("gs://")) {
      throw new Error("Invalid GCS URI format. Must start with gs://");
    }

    const uriParts = gcsUri.slice(5).split("/");
    const bucketName = uriParts[0];
    const fileName = uriParts.slice(1).join("/");

    console.log(`Bucket: ${bucketName}, File: ${fileName}`);

    // 1. Get Credentials from Secret Manager (reusing existing logic pattern if needed, or assuming default env auth)
    // For simplicity in this step, we'll rely on ADC or the same credentials if we were to load them.
    // However, the existing code loads credentials dynamically for the *backend service call*.
    // For GCS, we might need to do the same if not running in an env with implicit access.
    // Given Cloud Run usually has the service account attached, we'll try default auth first.
    // If that fails, we might need to reuse the secret manager logic, but usually Storage() works with ADC.

    const storage = new Storage();
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(fileName);

    const [exists] = await file.exists();
    if (!exists) {
      console.warn("⚠️ Config file not found in GCS.");
      res.status(404).json({ error: "Config file not found" });
      return;
    }

    const [content] = await file.download();
    const jsonContent = JSON.parse(content.toString());

    console.log("✅ Config loaded successfully.");
    res.json(jsonContent);

  } catch (err: any) {
    console.error("❌ GCS Fetch Error:", err.message);
    res.status(500).json({ error: "Failed to fetch current config", details: err.message });
  }
});

// Trigger Cloud Run Job
app.post("/api/trigger-job", async (req, res) => {
  console.log(`\n[${new Date().toISOString()}] 🚀 Triggering Cloud Run Job`);

  try {
    const result = await triggerJob();
    console.log(`✅ Job triggered successfully. Operation: ${result.operation}`);

    res.json({
      success: true,
      message: "Job triggered successfully",
      operation: result.operation,
      job: result.job
    });

  } catch (err: any) {
    if (err.code === 409) {
      console.warn(`🛑 BLOCKED: Job is already running (Execution: ${err.executionId})`);
      res.status(409).json({
        error: err.message,
        executionId: err.executionId,
        startTime: err.startTime
      });
    } else {
      console.error("❌ Job Trigger Error:", err.message);
      res.status(500).json({ error: "Failed to trigger job", details: err.message });
    }
  }
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