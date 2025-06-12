# Use a slim Python base image for smaller size
FROM python:3.11-slim-bookworm

# Set environment variables for non-interactive NLTK downloads and Python unbuffered output
ENV NLTK_DATA=/usr/local/nltk_data \
    PYTHONUNBUFFERED=1

# Create a non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

# Set the working directory inside the container
WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
# Use --no-cache-dir for smaller image size
RUN pip install --no-cache-dir -r requirements.txt

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
# Use the -A flag for aiorun to automatically manage the event loop
CMD ["aiorun", "--single-task", "--shutdown-timeout", "5.0", "main:main"]
