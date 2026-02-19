from flask import Flask, request, jsonify, session
import logging
from aim_waves.config import Config
from aim_waves.api.routes import api_bp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    app.register_blueprint(api_bp)

    ALLOWED_ENDPOINTS = {"api.login", "api.health", "static"}

    @app.before_request
    def protect_routes():
        if request.endpoint in ALLOWED_ENDPOINTS:
            return
        # Also allow OPTIONS for CORS if needed, or if endpoint is None (404/static handling)
        if request.endpoint is None:
            return
            
        if not session.get("is_authed"):
            return jsonify({"error": "Unauthorized"}), 401

    return app
