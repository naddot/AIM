# Use full Python 3.11 base image (includes build tools)
FROM python:3.11

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv

# Set working directory
WORKDIR /app

# Install system dependencies only for build steps
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && python -m venv $VIRTUAL_ENV \
    && $VIRTUAL_ENV/bin/pip install --upgrade pip

# Ensure virtualenv is used
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy Python dependencies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Remove build tools to slim the image
RUN apt-get purge -y --auto-remove build-essential gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

# (Optional) Verify resources exist - the recursive COPY . . handles directories
# But we can explicit if we want to be sure about caching layers
# COPY aim_waves/data/resources/segmentlist.csv aim_waves/data/resources/

# Expose the Flask/Gunicorn port
EXPOSE 8080

# Run the app using Gunicorn (2 workers, adjustable)
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--workers=2", "--timeout=600", "--threads=8", "run:app"]