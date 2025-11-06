FROM python:3.11-slim

# Install FFmpeg
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY ffmpeg_api_requirements.txt requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY ffmpeg_api_service.py app.py

# Create temp directories
RUN mkdir -p /tmp/uploads /tmp/outputs

# Expose port
EXPOSE 10000

# Run with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--workers", "2", "--timeout", "600", "app:app"]
