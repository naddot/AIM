FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .
COPY tyrescore_algorithm.sql .
COPY aim_cam_sku_update.sql .
COPY aim_dashboard_update.sql .
COPY aim_insights_update.sql .
COPY aim_analysis_update.sql .
COPY aim_merchandising_update.sql .
COPY aim_size_file_update.sql .

# Run the application
CMD ["python", "main.py"]
