FROM python:3.12-bullseye

# Install essentials
RUN apt update \
	&& apt install -y \
		build-essential \
		ca-certificates \
		curl \
		git \
		libssl-dev \
		software-properties-common

# Upgrade pip
RUN python -m pip install --upgrade pip

# Work in this folder
WORKDIR /project

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app configs
COPY app-configs app-configs

# Copy src files
COPY src .

# Set Python path
ENV PYTHONPATH=/project