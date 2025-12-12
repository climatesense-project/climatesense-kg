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

# Normalize Virtuoso ODBC driver path
RUN set -e; \
    DRIVER_X86="/usr/lib/x86_64-linux-gnu/odbc/virtodbc.so"; \
    DRIVER_ARM="/usr/lib/aarch64-linux-gnu/odbc/virtodbc.so"; \
    TARGET="/usr/lib/odbc/virtodbc.so"; \
    mkdir -p /usr/lib/odbc; \
    if [ -f "$DRIVER_X86" ]; then \
        ln -s "$DRIVER_X86" "$TARGET"; \
        echo "Linked x86_64 Virtuoso driver -> $TARGET"; \
    elif [ -f "$DRIVER_ARM" ]; then \
        ln -s "$DRIVER_ARM" "$TARGET"; \
        echo "Linked ARM64 Virtuoso driver -> $TARGET"; \
    else \
        echo "Error: Virtuoso ODBC driver not found on either architecture"; \
        find / -name "virtodbc*.so"; \
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
