import { NotebookParams, NotebookOutput } from "../types";

/**
 * Sends the configuration to the local backend proxy.
 */
export const saveConfigurationAndRun = async (params: NotebookParams): Promise<NotebookOutput> => {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 15000); // 15s timeout

  console.groupCollapsed("🚀 API Request: /api/save-config");

  try {
    // Construct Payload
    const limitSegmentsArray = params.LIMIT_TO_SEGMENTS
      ? params.LIMIT_TO_SEGMENTS.split(',').map(s => s.trim()).filter(s => s.length > 0)
      : [];

    const jsonPayload = {
      TOTAL_PER_SEGMENT: Number(params.TOTAL_PER_SEGMENT),
      GOLDILOCKS_ZONE_PCT: Number(params.GOLDILOCKS_ZONE_PCT),
      PRICE_FLUCTUATION_UPPER: Number(params.PRICE_FLUCTUATION_UPPER),
      PRICE_FLUCTUATION_LOWER: Number(params.PRICE_FLUCTUATION_LOWER),
      BRAND_ENHANCER: String(params.BRAND_ENHANCER || ""),
      MODEL_ENHANCER: String(params.MODEL_ENHANCER || ""),
      SEASON: String(params.SEASON || ""),
      LIMIT_TO_SEGMENTS: limitSegmentsArray
    };

    console.log("🌍 Origin:", window.location.origin);
    console.log("📦 Payload:", JSON.stringify(jsonPayload, null, 2));

    // Execute Request
    // We target our own server route /api/save-config, which proxies to the backend service.
    let apiResponse;
    try {
      apiResponse = await fetch('/api/save-config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(jsonPayload),
        signal: controller.signal
      });
    } catch (networkError: any) {
      if (networkError.name === 'AbortError') {
        throw new Error("Request timed out (15s). Is the server overloaded?");
      }
      throw new Error("Network connection failed. Backend may be offline.");
    } finally {
      clearTimeout(timeoutId);
    }

    // Handle Response
    console.log(`📥 Status: ${apiResponse.status} ${apiResponse.statusText}`);
    const rawText = await apiResponse.text();
    console.log("📄 Body:", rawText);

    let result;
    try {
      result = JSON.parse(rawText);
    } catch (e) {
      result = { message: rawText, error: "Non-JSON response" };
    }

    if (!apiResponse.ok) {
      const errorMsg = result.error || result.details || "Unknown Server Error";
      throw new Error(`Server Error (${apiResponse.status}): ${errorMsg}`);
    }

    console.groupEnd();
    return {
      success: true,
      message: result.message || "Configuration updated successfully.",
      timestamp: new Date().toLocaleTimeString()
    };

  } catch (error: any) {
    console.error("❌ Process Failed:", error);
    console.groupEnd();
    throw error;
  }
};

/**
 * Checks the status of the Cloud Run job.
 */
export const getJobStatus = async (): Promise<{
  isActive: boolean;
  executionId: string | null;
  state: string | null;
  startTime: string | null;
  lastChecked: string;
}> => {
  try {
    const response = await fetch('/api/job-status');
    if (!response.ok) {
      throw new Error(`Status check failed: ${response.statusText}`);
    }
    return await response.json();
  } catch (error) {
    console.error("Job status check failed:", error);
    // Default to inactive on error to allow user to try again (or handle error in UI)
    return {
      isActive: false,
      executionId: null,
      state: null,
      startTime: null,
      lastChecked: new Date().toISOString()
    };
  }
};