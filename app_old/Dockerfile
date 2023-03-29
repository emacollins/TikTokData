# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Install system dependencies for Playwright
RUN playwright install-deps

# Install Playwright browser dependencies
RUN python -m playwright install

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    bzip2 \
    libxshmfence1 \
    libdrm2 \
    libgbm1 \
    libx11-xcb1 \
    libasound2 \
    libdbus-glib-1-2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcb-shm0 \
    libxt6 \
    && rm -rf /var/lib/apt/lists/*

# Install Firefox
RUN wget "https://download-installer.cdn.mozilla.net/pub/firefox/releases/97.0.1/linux-x86_64/en-US/firefox-97.0.1.tar.bz2" && \
    tar xjf firefox-97.0.1.tar.bz2 && \
    mv firefox /usr/local/bin/ && \
    ln -s /usr/local/bin/firefox/firefox /usr/local/bin/firefox-bin && \
    rm firefox-97.0.1.tar.bz2

# Install the Playwright browsers
RUN playwright install firefox

RUN npx playwright install

# Run run.py when the container launches
CMD ["python", "run.py"]
