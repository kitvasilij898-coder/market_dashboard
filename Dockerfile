FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server.py ws_logger.py market_discovery.py index.html ./

# Create data directory for CSV files
RUN mkdir -p /app/data

# Expose the dashboard port
EXPOSE 8765

# Run the server
CMD ["python", "server.py"]
