from flask import Blueprint, request, jsonify, session
from aim_waves.core.engine import (
    generate_batch_recommendations, 
    generate_recommendations_batch_push,
    START_TIME
)
from aim_waves.data.loader import vehicle_size_map
from aim_waves.config import Config
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

api_bp = Blueprint('api', __name__)

@api_bp.route("/login", methods=["POST"])
def login():
    pwd = request.form.get("password", "")
    if not Config.APP_ACCESS_PASSWORD:
         return jsonify({"error": "Server config missing password"}), 500
    
    if pwd == Config.APP_ACCESS_PASSWORD:
        session["is_authed"] = True
        session.permanent = True
        return jsonify({"status": "authenticated", "message": "Login successful"}), 200
    else:
        return jsonify({"error": "Incorrect password"}), 401

@api_bp.route("/api/recommendations/batch", methods=["POST"])
def api_recommendations_batch():
    """
    NEW: Push Batch Endpoint.
    Growth Job sends a list of CAMs to process.
    """
    payload = request.json
    if not payload:
        return jsonify({"error": "Missing JSON payload"}), 400
    
    run_id = payload.get("run_id")
    cams = payload.get("cams")
    params = payload.get("params", {})

    if not run_id or not cams:
        return jsonify({"error": "Missing required fields: run_id, cams"}), 400
    
    if not isinstance(cams, list):
        return jsonify({"error": "cams must be a list"}), 400

    if len(cams) > 500:
        return jsonify({"error": "Batch size exceeds limit of 500"}), 400

    logger.info(f"🚀 Processing batch for run_id: {run_id} ({len(cams)} CAMs)")
    
    results = generate_recommendations_batch_push(run_id, cams, params)
    return jsonify(results)

@api_bp.route("/api/status/engine")
def api_status_engine():
    """Diagnostic info about the compute engine."""
    return jsonify({
        "status": "online",
        "build_version": "2.1.0-cost-control",
        "start_time": START_TIME.isoformat(),
        "uptime_seconds": (datetime.now() - START_TIME).total_seconds(),
        "concurrency": {
            "batch_max_cams": 500,
            "parallel_vehicles": 5,
            "sku_workers_per_vehicle": 8
        }
    })

@api_bp.route("/api/recommendations")
def api_recommendations():
    """DEPRECATED: Support for legacy paging runner."""
    logger.warning("⚠️ Call to DEPRECATED /api/recommendations endpoint.")
    try:
        top_n = int(request.args.get("top_n", "100"))
    except ValueError:
        top_n = 100
    # ... rest of legacy logic clipped for brevity, calling existing engine func ...
    data = generate_batch_recommendations(
        top_n=top_n,
        goldilocks_zone_pct=float(request.args.get("goldilocks_zone_pct", Config.DEFAULT_GOLDILOCKS_PCT)),
        price_fluctuation_upper=float(request.args.get("price_fluctuation_upper", Config.DEFAULT_PRICE_FLUCTUATION_UPPER)),
        price_fluctuation_lower=float(request.args.get("price_fluctuation_lower", Config.DEFAULT_PRICE_FLUCTUATION_LOWER)),
        brand_enhancer=request.args.get("brand_enhancer"),
        model_enhancer=request.args.get("model_enhancer"),
        pod_filter=request.args.get("pod"),
        segment_filter=request.args.get("segment"),
        seasonal_performance=request.args.get("season"),
        offset=int(request.args.get("offset", "0")),
    )
    return jsonify(data)

@api_bp.route("/get_sizes/<vehicle>")
def get_sizes(vehicle):
    sizes = vehicle_size_map.get(vehicle.upper(), [])
    def size_key(s):
        parts = re.findall(r'\d+', s)
        return [int(p) for p in parts] if parts else [0]
    return jsonify(sorted(set(sizes), key=size_key))

@api_bp.route("/health")
def health():
    return "OK", 200
