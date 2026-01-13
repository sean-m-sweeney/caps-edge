# Stage 1: Build Tailwind CSS
FROM node:20-slim AS css-builder

WORKDIR /build

# Copy package files and install
COPY package.json tailwind.config.js ./
RUN npm install

# Copy frontend source and build CSS
COPY frontend/ ./frontend/
RUN npm run build:css

# Stage 2: Python application
FROM python:3.11-slim

WORKDIR /app

# Install cron and other dependencies
RUN apt-get update && apt-get install -y curl \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY scripts/ ./scripts/
COPY crontab /etc/cron.d/chel-edge-cron

# Copy built CSS from builder stage (overwrites the source styles.css)
COPY --from=css-builder /build/frontend/styles.css ./frontend/styles.css

# Set up cron
RUN chmod 0644 /etc/cron.d/chel-edge-cron && \
    crontab /etc/cron.d/chel-edge-cron

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV DATA_DIR=/app/data
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8000

# Create startup script
RUN echo '#!/bin/bash\n\
# Start cron in background\n\
service cron start\n\
\n\
# Run initial data refresh if database is empty\n\
python -c "from backend.database import get_all_players_with_stats; exit(0 if get_all_players_with_stats() else 1)" 2>/dev/null || \
    python scripts/refresh.py\n\
\n\
# Start the FastAPI server\n\
exec uvicorn backend.main:app --host 0.0.0.0 --port 8000\n' > /app/start.sh && \
    chmod +x /app/start.sh

# Run the startup script
CMD ["/app/start.sh"]
