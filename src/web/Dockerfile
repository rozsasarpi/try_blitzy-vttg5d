# syntax=docker/dockerfile:1

# Use a multi-stage build to reduce the final image size
# Builder stage: Install dependencies and prepare the application
FROM python:3.10-slim AS builder

# Set environment variables
# Prevents Python from writing .pyc files to disk (reduces image size)
ENV PYTHONDONTWRITEBYTECODE=1
# Ensures that the Python output is sent straight to the terminal without buffering
ENV PYTHONUNBUFFERED=1
# Disables the pip cache
ENV PIP_NO_CACHE_DIR=off
# Disables pip version check to avoid warnings
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
# Set a default timeout for pip
ENV PIP_DEFAULT_TIMEOUT=100

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Final stage: Copy necessary runtime components
FROM python:3.10-slim AS final

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Set the Python path to include the application directory
ENV PYTHONPATH=/app
# Set the timezone to America/Chicago
ENV TZ=America/Chicago

# Set working directory
WORKDIR /app

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN groupadd -r dashboard && useradd -r -g dashboard dashboard

# Create directories for cache and logs
RUN mkdir -p /app/cache /app/logs

# Set ownership of cache and logs directories to the dashboard user
RUN chown -R dashboard:dashboard /app

# Copy installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

# Copy application code
COPY . .

# Switch to the non-root user
USER dashboard

# Expose port 8050
EXPOSE 8050

# Healthcheck to ensure the application is running
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=30s CMD curl -f http://localhost:8050/ || exit 1

# Command to run the application using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "--timeout", "120", "wsgi:server"]