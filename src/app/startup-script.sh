#!/bin/bash

# startup_script.sh

# Exit immediately if a command exits with a non-zero status
set -e

# Update and install dependencies
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv

# Set up the application directory
APP_DIR="/opt/llm-fine-tuning"
mkdir -p $APP_DIR
cd $APP_DIR

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Clone the application repository (replace with your actual repository URL)
git clone https://github.com/your-username/llm-fine-tuning-scheduler.git .

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export PROJECT_ID=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)
export VM_NAME=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
export JOB_SUBSCRIPTION="llm-fine-tuning-jobs-sub"
export VM_UPDATES_TOPIC="vm-status-updates"
export STOP_SIGNAL_SUBSCRIPTION="job-stop-signals-sub"
export RESULTS_BUCKET="llm-fine-tuning-results"

# Additional environment variables can be set here or fetched from Secret Manager

# Start the job executor
python job_executor.py >> /var/log/job_executor.log 2>&1 &

# Set up log rotation
cat <<EOF > /etc/logrotate.d/job_executor
/var/log/job_executor.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
}
EOF

# Set up a cleanup script to run on shutdown
cat <<EOF > /usr/local/bin/cleanup.sh
#!/bin/bash
pkill -f job_executor.py
# Add any additional cleanup steps here
EOF

chmod +x /usr/local/bin/cleanup.sh

# Register the cleanup script to run on shutdown
if [ -d /etc/rc0.d ]; then
    ln -s /usr/local/bin/cleanup.sh /etc/rc0.d/K01cleanup_job_executor
fi

# Enable stackdriver logging
curl -sSO https://dl.google.com/cloudagents/add-logging-agent-repo.sh
sudo bash add-logging-agent-repo.sh --also-install

# Configure the logging agent to pick up job executor logs
cat <<EOF > /etc/google-fluentd/config.d/job_executor.conf
<source>
  type tail
  format none
  path /var/log/job_executor.log
  pos_file /var/lib/google-fluentd/pos/job_executor.pos
  read_from_head true
  tag job_executor
</source>
EOF

# Restart the logging agent to pick up the new configuration
sudo service google-fluentd restart

echo "Startup script completed."