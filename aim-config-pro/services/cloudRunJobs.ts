import { spawn } from "child_process";
import { JobsClient, ExecutionsClient } from "@google-cloud/run";
import { Logging } from "@google-cloud/logging";

interface JobStatus {
    isActive: boolean;
    executionId: string | null;
    state: string | null;
    startTime: string | null;
    lastChecked: string;
    currentStage?: string;
    logs?: Array<{ time: string, level: string, msg: string }>;
    latestLogTimestamp?: string | null;
}

const PROJECT_ID = process.env.PROJECT_ID || "bqsqltesting";
const REGION = process.env.REGION || "europe-west2";
const JOB_NAME = process.env.JOB_NAME || "aim-growth-job";

// ----------------------------------------------------------------------
// ENVIRONMENT DETECTION
// ----------------------------------------------------------------------
// Use gcloud CLI if:
// 1. USE_GCLOUD_CLI env var is explicitly "true" (useful for WSL/Linux dev)
// 2. OR we are on Windows and USE_GCLOUD_CLI is NOT "false" (default behavior)
const USE_GCLOUD_CLI = process.env.USE_GCLOUD_CLI === "true";
const IS_WINDOWS_DEFAULT = process.platform === "win32" && process.env.USE_GCLOUD_CLI !== "false";
const USE_LOCAL_CLI = USE_GCLOUD_CLI || IS_WINDOWS_DEFAULT;

console.log(`[CloudRunService] Environment Strategy: ${USE_LOCAL_CLI ? "Local CLI (gcloud)" : "Native SDK (Production)"}`);

// ----------------------------------------------------------------------
// STRATEGY 1: NATIVE SDK (Production / Linux)
// ----------------------------------------------------------------------
const executionsClient = new ExecutionsClient({ projectId: PROJECT_ID });
const jobsClient = new JobsClient({ projectId: PROJECT_ID });
const logging = new Logging({ projectId: PROJECT_ID });

async function sdkGetExecutionLogs(executionId: string, since?: string) {
    // Filter for execution using the reliable dot-label form
    const baseFilter = `resource.type="cloud_run_job" AND resource.labels.job_name="${JOB_NAME}" AND resource.labels.location="${REGION}" AND labels."run.googleapis.com/execution_name"="${executionId}"`;
    const timeFilter = since ? ` AND timestamp > "${since}"` : "";

    // In prod, logs might be in stdout/err. Query both.
    const logFilter = `
        ${baseFilter} ${timeFilter}
        (
            logName="projects/${PROJECT_ID}/logs/run.googleapis.com%2Fstdout"
            OR
            logName="projects/${PROJECT_ID}/logs/run.googleapis.com%2Fstderr"
        )
    `;

    try {
        const [entries] = await logging.getEntries({
            filter: logFilter,
            orderBy: 'timestamp desc',
            pageSize: 50,
        });

        // Convert to common format
        const logs = entries.map(entry => {
            let msg = "";
            if (typeof entry.data === 'string') {
                msg = entry.data;
            } else if (entry.data && typeof entry.data === 'object') {
                const payload = entry.data as any;
                msg = payload.message || payload.msg || JSON.stringify(payload);
            } else if ((entry.metadata as any).textPayload) {
                msg = (entry.metadata as any).textPayload;
            }

            return {
                time: entry.metadata.timestamp ? new Date(entry.metadata.timestamp as any).toLocaleTimeString('en-GB') : '',
                timestamp: entry.metadata.timestamp ? new Date(entry.metadata.timestamp as any).toISOString() : new Date().toISOString(),
                level: (entry.metadata.severity as string) || "INFO",
                msg: msg || ""
            };
        });

        return logs;
    } catch (error: any) {
        console.error("[SDK] Log fetch failed:", error.message);
        return [];
    }
}

async function sdkGetActiveExecution(since?: string): Promise<JobStatus> {
    try {
        const parent = `projects/${PROJECT_ID}/locations/${REGION}/jobs/${JOB_NAME}`;
        const [executions] = await executionsClient.listExecutions({ parent });

        // Sort newest first
        executions.sort((a, b) => {
            const tA = Number(a.createTime?.seconds || 0);
            const tB = Number(b.createTime?.seconds || 0);
            return tB - tA;
        });

        for (const execution of executions) {
            // Extract short ID correctly
            const execResourceName = execution.name || "unknown";
            const execId = execResourceName.split('/').pop() || execResourceName;

            if (!execution.completionTime) {
                console.log(`[SDK] Active execution found: ${execId}`);

                const logs = await sdkGetExecutionLogs(execId, since);
                const currentStage = inferStageFromEntries(logs);
                const latestLogTimestamp = logs.length > 0 ? logs[0].timestamp : (since || null);

                return {
                    isActive: true,
                    executionId: execId,
                    state: "RUNNING",
                    startTime: execution.createTime ? new Date(Number(execution.createTime.seconds) * 1000).toISOString() : null,
                    lastChecked: new Date().toISOString(),
                    currentStage: currentStage === "unknown" ? "stage1" : currentStage,
                    logs: logs.map(l => ({ time: l.time, level: l.level, msg: l.msg })),
                    latestLogTimestamp
                };
            }
        }

        return { isActive: false, executionId: null, state: null, startTime: null, lastChecked: new Date().toISOString() };
    } catch (error: any) {
        console.error("[SDK] Failed to check status:", error);
        throw error;
    }
}

async function sdkTriggerJob() {
    const parent = `projects/${PROJECT_ID}/locations/${REGION}/jobs/${JOB_NAME}`;
    const [op] = await jobsClient.runJob({ name: parent });
    return { operation: op.name, job: parent };
}


// ----------------------------------------------------------------------
// ----------------------------------------------------------------------
// Force the correct path found via 'where gcloud', ignoring potentially polluted env vars
const GCLOUD_BINARY = process.env.GCLOUD_PATH || "C:\\Users\\otsdato\\AppData\\Local\\Google\\Cloud SDK\\google-cloud-sdk\\bin\\gcloud.cmd";

function psQuote(s: string) {
    // Simple litmus test: fast path for simple flags
    // Only quote if it contains spaces or special characters like " ' ( )
    if (/[\s"'()]/.test(s)) {
        return `'${s.replace(/'/g, "''")}'`;
    }
    return s;
}

function runGcloudPowerShell(args: string[]): Promise<{ stdout: string; stderr: string; code: number }> {
    if (!USE_LOCAL_CLI) {
        return Promise.reject(new Error("CLI execution disabled in this environment"));
    }

    return new Promise((resolve, reject) => {
        const cmd = `& ${psQuote(GCLOUD_BINARY)} ${args.map(psQuote).join(" ")}`;
        console.log("[GCLOUD]", cmd);

        const child = spawn("powershell.exe", ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", cmd], {
            windowsHide: true, env: process.env
        });

        let stdout = "", stderr = "";
        child.stdout.on("data", d => stdout += d.toString());
        child.stderr.on("data", d => stderr += d.toString());

        child.on("close", code => {
            console.log("[GCLOUD] exit", code, "stdout", stdout.length, "stderr", stderr.length);
            // We resolve even on error code to allow caller to handle stderr specifics if needed, 
            // but for safety in existing pattern, lets reject if code != 0
            if (code !== 0) reject(new Error(`gcloud failed (code ${code}): ${stderr || stdout}`));
            else resolve({ stdout, stderr, code: code ?? 0 });
        });
        child.on("error", reject);
    });
}

async function localGetExecutionLogs(executionId: string, since?: string) {
    // Revert to dot syntax as this is stable in gcloud CLI
    const baseFilter = `resource.type="cloud_run_job" AND resource.labels.job_name="${JOB_NAME}" AND resource.labels.location="${REGION}" AND labels."run.googleapis.com/execution_name"="${executionId}"`;
    const timeFilter = since ? ` AND timestamp > "${since}"` : "";

    // Explicitly check both streams
    const filter = `${baseFilter}${timeFilter} AND (logName="projects/${PROJECT_ID}/logs/run.googleapis.com%2Fstdout" OR logName="projects/${PROJECT_ID}/logs/run.googleapis.com%2Fstderr")`;

    try {
        const { stdout } = await runGcloudPowerShell([
            "logging", "read", filter,
            `--project=${PROJECT_ID}`, "--format=json", "--limit=200", "--order=desc", "--freshness=24h"
        ]);

        let entries: any[] = [];
        try {
            entries = JSON.parse(stdout || "[]");
        } catch (parseErr) {
            console.error("[LocalCLI] JSON Parse Error for logs:", parseErr);
            console.error("[LocalCLI] First 500 chars of stdout:", stdout.substring(0, 500));
            return [];
        }

        return entries.map((entry: any) => {
            let msg = entry.textPayload
                || entry.jsonPayload?.message
                || entry.jsonPayload?.msg
                || entry.jsonPayload?.log
                || (typeof entry.jsonPayload === "string" ? entry.jsonPayload : JSON.stringify(entry.jsonPayload ?? entry.protoPayload ?? entry));

            if (!msg) msg = "";

            return {
                time: entry.timestamp ? new Date(entry.timestamp).toLocaleTimeString('en-GB') : '',
                timestamp: entry.timestamp,
                level: entry.severity || "INFO",
                msg
            };
        });
    } catch (e: any) {
        console.error("[LocalCLI] Log read failed:", e.message);
        return [];
    }
}

async function localGetActiveExecution(since?: string): Promise<JobStatus> {
    try {
        const { stdout } = await runGcloudPowerShell([
            "run", "jobs", "executions", "list",
            `--job=${JOB_NAME}`, `--region=${REGION}`, `--project=${PROJECT_ID}`,
            "--format=json", "--limit=5", "--sort-by=~creationTimestamp"
        ]);

        let executions: any[] = [];
        try {
            executions = JSON.parse(stdout || "[]");
        } catch (parseErr) {
            console.error("[LocalCLI] JSON Parse Error for executions:", parseErr);
            console.error("[LocalCLI] First 500 chars of stdout:", stdout.substring(0, 500));
            throw new Error("Failed to parse executions list from gcloud");
        }

        for (const execution of executions) {
            // Correctly extract short ID from full resource name
            const execResourceName = execution.metadata?.name || "unknown";
            const execId = execResourceName.split("/").pop() || execResourceName;

            if (!execution.status?.completionTime) {
                console.log(`[LocalCLI] Active execution: ${execId} (Resource: ${execResourceName})`);

                const logs = await localGetExecutionLogs(execId, since);
                const currentStage = inferStageFromEntries(logs);
                const latestLogTimestamp = logs.length > 0 ? logs[0].timestamp : (since || null);

                return {
                    isActive: true,
                    executionId: execId,
                    state: "RUNNING",
                    startTime: execution.status?.startTime || execution.metadata?.creationTimestamp,
                    lastChecked: new Date().toISOString(),
                    currentStage: currentStage === "unknown" ? "stage1" : currentStage,
                    logs: logs.map((l: any) => ({ time: l.time, level: l.level, msg: l.msg })),
                    latestLogTimestamp
                };
            }
        }
        return { isActive: false, executionId: null, state: null, startTime: null, lastChecked: new Date().toISOString() };
    } catch (error: any) {
        console.error("[LocalCLI] Status check failed:", error.message);
        throw error;
    }
}

async function localTriggerJob() {
    console.log(`[LocalCLI] Triggering ${JOB_NAME}...`);
    const { stdout } = await runGcloudPowerShell([
        "run", "jobs", "execute", JOB_NAME,
        `--region=${REGION}`, `--project=${PROJECT_ID}`, "--format=json"
    ]);
    const result = JSON.parse(stdout || "{}");
    const executionName = result.metadata?.name;
    const shortId = executionName ? executionName.split("/").pop() : executionName;

    return { operation: shortId, job: JOB_NAME };
}

// ----------------------------------------------------------------------
// SHARED UTILS (Stage Detection)
// ----------------------------------------------------------------------
function inferStageFromEntries(logs: { msg: string }[]) {
    // Determine stage from latest log entry (logs are desc)
    const chronologicalLogs = [...logs].reverse();
    let stage = "unknown";
    for (const log of chronologicalLogs) {
        const m = log.msg.match(/Starting Stage\s+(\d+)/i);
        if (m) stage = `stage${m[1]}`;
    }
    return stage;
}

// ----------------------------------------------------------------------
// MAIN EXPORTS (Switch Logic)
// ----------------------------------------------------------------------

export async function getActiveExecution(since?: string) {
    if (USE_LOCAL_CLI) return localGetActiveExecution(since);
    return sdkGetActiveExecution(since);
}

export async function triggerJob() {
    // Concurrency check based on current env
    const currentStatus = await getActiveExecution();
    if (currentStatus.isActive) {
        throw { code: 409, message: "Job is already running", executionId: currentStatus.executionId, startTime: currentStatus.startTime };
    }

    if (USE_LOCAL_CLI) return localTriggerJob();
    return sdkTriggerJob();
}
