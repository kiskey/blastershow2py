# Use a slim Python base image for smaller size
FROM python:3.11-slim-bookworm

# Set environment variables for non-interactive NLTK downloads and Python unbuffered output
# Add /usr/local/bin to PATH to ensure pip-installed executables are found
ENV NLTK_DATA=/usr/local/nltk_data \
    PYTHONUNBUFFERED=1 \
    PATH="/usr/local/bin:$PATH"

# Create a non-root user and add them to the system group
RUN addgroup --system appgroup && \
    adduser --system --disabled-password --gecos "" --no-create-home appuser && \
    adduser appuser appgroup

# Set the working directory inside the container
WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Install build dependencies required for some Python packages
# and then remove them to keep the final image size small.
# python3-dev provides the necessary Python header files
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential python3-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y build-essential python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Download NLTK data required for text normalization
# These commands need to run before copying the application code
# as they will be run by root, and then the NLTK_DATA directory will be
# made available to the appuser.
RUN python -c "import nltk; nltk.download('punkt', download_dir='/usr/local/nltk_data')" && \
    python -c "import nltk; nltk.download('wordnet', download_dir='/usr/local/nltk_data')" && \
    python -c "import nltk; nltk.download('stopwords', download_dir='/usr/local/nltk_data')" && \
    python -c "import nltk; nltk.download('averaged_perceptron_tagger', download_dir='/usr/local/nltk_data')"

# Copy the rest of the application code
COPY . .

# Set ownership of the /app directory to the appuser
RUN chown -R appuser:appgroup /app

# Switch to the non-root user
USER appuser

# Expose the port our aiohttp server will run on
EXPOSE 8080

# Define the command to run the application
# Now that /usr/local/bin is explicitly in PATH, 'aiorun' should be found correctly.
CMD ["aiorun", "--single-task", "--shutdown-timeout", "5.0", "main:main"]
