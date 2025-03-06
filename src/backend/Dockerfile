# Use a multi-stage build to reduce the final image size
# Builder stage: Install dependencies and prepare the application
FROM python:3.10-slim AS builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=off
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_DEFAULT_TIMEOUT=100

# Set working directory
WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN python -m pip install --upgrade pip

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Final stage: Copy only necessary runtime components
FROM python:3.10-slim AS final

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Set working directory
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -r forecaster && useradd -r -g forecaster forecaster

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs

# Set ownership of /app to the forecaster user
RUN chown -R forecaster:forecaster /app

# Copy only the necessary files from the builder stage
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /app .

# Switch to the non-root user
USER forecaster

# Expose port
EXPOSE 8000

# Define volume mounts
VOLUME /app/data
VOLUME /app/logs

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=30s CMD curl -f http://localhost:8000/api/health || exit 1

# Command to run the application
CMD ["python", "-m", "main"]