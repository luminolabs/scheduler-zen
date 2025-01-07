# Scheduler Zen Infrastructure

This directory contains the Terraform configuration for Scheduler Zen's cloud infrastructure on Google Cloud Platform (GCP).

The infrastructure is designed to support scalable machine learning job processing across multiple regions and GPU types.

## Directory Structure

```
terraform/
├── compute.tf              # Compute engine VM configurations 
├── config-example.env      # Example application configuration variables template
├── db.tf                   # Cloud SQL database configurations
├── dev.tfvars              # Terraform variables for dev environment
├── dev-config.env          # Application configuration variables for dev environment (not version controlled)
├── main.tf                 # Main Terraform configuration and provider setup
├── permissions.tf          # IAM permissions and roles configuration
├── pubsub.tf               # Pub/Sub topics and subscriptions  
├── pubsub.tf               # Pub/Sub topics and subscriptions
├── secrets.tf              # Secret Manager configuration
├── secrets.tfvars          # Terraform secrets variables (not version controlled)
├── secrets-example.tfvars  # Example Terraform secrets variables template
├── variables.tf            # Variable definitions
```

## Core Components

### Compute Engine VM
- Defined in `compute.tf`
- Single VM running the scheduler service
- Uses custom VM image with Docker and monitoring
- Network and security configurations for internal access
- Metadata for environment variables and service configuration

### Cloud SQL Database
- Defined in `db.tf`
- Private PostgreSQL instance
- Network peering for secure access
- Automatic backups and maintenance windows
- Custom database user and password management
- Used for Scheduler and Customer API

### Pub/Sub Topics & Subscriptions
- Defined in `pubsub.tf`
- Job control and monitoring topics:
    - Heartbeats subscription (`pipeline-zen-jobs-heartbeats-scheduler`)
    - Local development subscription (`pipeline-zen-jobs-heartbeats-local-scheduler`)
- Message retention and retry policies

### IAM & Permissions
- Defined in `permissions.tf`
- Custom service account for scheduler service
- Custom IAM role for MIG management
- Project-level permissions:
    - Logging and monitoring
    - Secret Manager access
    - Pub/Sub publishing/subscribing
    - Cloud SQL client
- Storage bucket access for job results
- Artifact Registry access for Docker images

## Design Patterns

### 1. Environment Separation
- Environment-specific variable files (e.g., dev.tfvars)
- Environment variables in VM metadata
- Environment-aware resource naming

### 2. Secret Management
- Application secrets stored in Secret Manager
- Database credentials managed through terraform variables
- Clear separation between Terraform and application secrets

### 3. Private Networking
- Private IP for Cloud SQL instance
- VPC peering for service communication
- No public IP exposure for core services

## Infrastructure Deployment

### Prerequisites
1. Install OpenTofu
2. GCP project access and credentials
3. Required environment files:
    - `{env}-config.env` - Application configuration for target environment
    - `secrets.tfvars` - Terraform secrets, same for all environments
    - `{env}.tfvars` - Environment variables for target environment

### Configuration Files
1. Copy `config-example.env` to `{env}-config.env`
2. Copy `secrets-example.tfvars` to `secrets.tfvars`
3. Set required variables in both files

### Deploy Infrastructure

```bash
# Initialize Terraform
tofu init

# Plan deployment
tofu plan -var-file="dev.tfvars" -var-file="secrets.tfvars"

# Apply changes
tofu apply -var-file="dev.tfvars" -var-file="secrets.tfvars"
```

## Development Guidelines

### Making Changes
1. Make infrastructure changes in feature branches
2. Test changes in development environment first
3. Use consistent naming conventions
4. Update documentation with new components
5. Consider impact on running services

## Security Best Practices

### Access Control
- Follow principle of the least privilege
- Use service accounts with minimal required permissions

### Secret Management
- Never commit secrets to version control
- Use Secret Manager for application secrets