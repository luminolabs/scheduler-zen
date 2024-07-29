# Running Pipeline Zen Jobs Scheduler

This document provides instructions for running the Pipeline Zen Jobs Scheduler locally and setting it up on a Google Cloud Platform (GCP) Virtual Machine (VM).

## Running Locally

Follow these steps to run the project on your local machine:

1. Navigate to the project root directory.

2. Create an `.env` file in the project root and add the following line:
   ```
   SZ_ENV=local
   ```

3. Install the required Python dependencies:
   ```
   pip install -Ur requirements.txt
   ```

4. Set the Python path:
   ```
   export PYTHONPATH=$(pwd)/src
   ```
   
5. Start the database:
   ```
   docker compose up -d db 
   ```

6. Start the scheduler and API:
   ```
   python src/app/api.py
   ```

## Setting Up on a GCP VM

To set up the project on a GCP VM, follow these steps:

1. Ensure that the VM is configured with the `scheduler-zen-dev` service account.

2. Install the GCP Ops Agent on the VM.

3. Set up log streaming to GCP. Refer to the configuration under the `infra/vm/` directory.

4. Clone the `scheduler-zen` repository to the `/scheduler-zen` directory on the VM.

5. Navigate to the project root:
   ```
   cd /scheduler-zen
   ```

6. Update the `/etc/environment` file with the following environment variables:
   ```
   SZ_ENV=dev
   PYTHONPATH=/scheduler-zen/src:/scheduler-zen/.__pylibs__:$PYTHONPATH
   ```

7. Export the environment variables:
   ```
   export SZ_ENV=dev
   export PYTHONPATH=/scheduler-zen/src:/scheduler-zen/.__pylibs__:$PYTHONPATH
   ```

8. Install Python dependencies:
   ```
   pip install --target=./.__pylibs__ -Ur requirements.txt
   ```

9. Start the database:
   ```
   docker compose up -d db 
   ```

10. Start a new `tmux` session:
    ```
    tmux
    ```

11. Within the `tmux` session, start the scheduler and API:
    ```
    python src/app/api.py
    ```

12. Check for any errors in the console output.

13. Detach from the `tmux` session by pressing `Ctrl-B` and then `D`.

14. Verify that logs are being streamed to the GCP logging service.

## Troubleshooting

If you encounter any issues during setup or execution:

1. Check that all environment variables are set correctly.
2. Ensure that the GCP service account has the necessary permissions.
3. Verify that all required dependencies are installed.
4. Check the GCP logging service for any error messages.

For more detailed information on using the Pipeline Zen Jobs Scheduler, please refer to the [USAGE.md](USAGE.md) file.