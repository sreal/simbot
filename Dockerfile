# Multi-stage build for Simbot
# Supports both Slack bot and MCP server

# Stage 1: Build dependencies
FROM python:3.11-slim AS builder

# Install system dependencies for pyodbc and ODBC drivers
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy dependency files and README (required by pyproject.toml)
COPY pyproject.toml uv.lock README.md ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Stage 2: Runtime
FROM python:3.11-slim

# Install runtime dependencies for ODBC
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server (runtime)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Create app user (don't run as root)
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy Python virtual environment from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv

# Copy application code
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser pyproject.toml ./

# Set Python to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV VIRTUAL_ENV=/app/.venv
ENV PYTHONPATH=/app/src

# Create logs directory
RUN mkdir -p logs && chown appuser:appuser logs

# Create queries directory (will be mounted as volume)
RUN mkdir -p queries && chown appuser:appuser queries

# Switch to app user
USER appuser

# Health check endpoint (for MCP server HTTP mode)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD [ -f /tmp/healthy ] || exit 1

# Default command (override in docker-compose)
CMD ["python", "-m", "simbot.main", "--help"]
