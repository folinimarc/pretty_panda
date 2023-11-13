# Multi-stage build with a common base stage and two stages for dev and prod

# docker build --platform linux/amd64 -t bonjour .
# docker run -it --rm -p 8888:8888 bonjour

# ############################
# BASE STAGE
# ############################

ARG GDAL_VERSION=3.7.3

FROM ghcr.io/osgeo/gdal:ubuntu-small-${GDAL_VERSION} as base

# Set the working directory in the container
WORKDIR /app

# Set timezone
# https://askubuntu.com/questions/909277/avoiding-user-interaction-with-tzdata-when-installing-certbot-in-a-docker-contai
RUN ln -snf /usr/share/zoneinfo/$CONTAINER_TIMEZONE /etc/localtime && echo $CONTAINER_TIMEZONE > /etc/timezone

# Install git
RUN apt update && \
    apt-get install -y --no-install-recommends \
    git

# Install python packages.
# Add directive for shapely and pygeos to use existing
# library (requires python3-dev and build-essentials).
RUN apt update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    python3-pip \
    libgeos-dev
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

#-----------------
# Misc setup
#-----------------
# Make sure jupyter lab terminal uses bash
ENV SHELL=/bin/bash

#-----------------
# Clean up
#-----------------
RUN apt-get purge -y build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# ############################
# DEV STAGE
# ############################

FROM base as dev

# Install dev python packages
COPY ./dev/requirements.dev.txt .
RUN pip install --no-cache-dir -r requirements.dev.txt

CMD ["jupyter", "lab" ,"--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser", "--autoreload"]
