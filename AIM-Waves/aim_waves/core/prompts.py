from aim_waves.core.utils import normalize_string_for_comparison
from aim_waves.config import Config
from jinja2 import Environment, FileSystemLoader
import logging

logger = logging.getLogger(__name__)

# Initialize Jinja2 Env
jinja_env = Environment(loader=FileSystemLoader(Config.PROMPT_TEMPLATE_DIR))

def get_error_output(vehicle, size, error_type="Error"):
    safe_vehicle = (vehicle or "UNKNOWN").strip().replace(" ", "_")
    safe_size = (size or "UNKNOWN").strip().replace("/", "-").replace(" ", "_")
    return f"{safe_vehicle} {safe_size} {error_type} {error_type} {error_type} {error_type} {' '.join(['-' for _ in range(20)])}"

def construct_prompt(vehicle, size, tyre_data_str, brand_enhancer_text, model_enhancer_lower, model_enhancer_text, seasonal_performance, season_enhancer_text, goldilocks_zone_pct, price_fluctuation_upper, price_fluctuation_lower):
    try:
        template = jinja_env.get_template("recommendation_prompt.j2")
        return template.render(
            vehicle=vehicle,
            size=size,
            tyre_data_str=tyre_data_str,
            brand_enhancer_text=brand_enhancer_text,
            model_enhancer_text=model_enhancer_text,
            season_enhancer_text=season_enhancer_text,
            goldilocks_zone_pct=goldilocks_zone_pct,
            price_fluctuation_upper=price_fluctuation_upper,
            price_fluctuation_lower=price_fluctuation_lower
        )
    except Exception as e:
        logger.error(f"❌ Failed to render prompt template: {e}")
        return ""
