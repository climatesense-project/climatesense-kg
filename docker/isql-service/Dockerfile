FROM python:3.11-bookworm

# Set working directory
WORKDIR /app

# Install required ODBC libraries
RUN apt-get update && apt-get install -y \
    libiodbc2 \
    libvirtodbc0 \
    unixodbc \
    unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify that the required ODBC driver exists
RUN if [ ! -f /usr/lib/x86_64-linux-gnu/odbc/virtodbc.so ]; then \
    echo "Error: Required ODBC driver not found at /usr/lib/x86_64-linux-gnu/odbc/virtodbc.so" && \
    exit 1; \
    fi

# Copy requirements file
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files
COPY . .

# Expose the specified port
EXPOSE 50118

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHON_ENV=production

# Use a non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Start the application
CMD ["python", "src/server.py"]
