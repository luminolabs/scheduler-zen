# TESTS.md - Documentation of Test Cases

This document provides an overview of all test cases implemented in the Pipeline Zen Jobs Scheduler project.

## Table of Contents
1. [test_api.py](#test_apipy)
2. [test_cluster_manager.py](#test_cluster_managerpy)
3. [test_cluster_orchestrator.py](#test_cluster_orchestratorpy)
4. [test_config_manager.py](#test_config_managerpy)
5. [test_fake_mig_manager.py](#test_fake_mig_managerpy)
6. [test_scheduler.py](#test_schedulerpy)
7. [test_utils.py](#test_utilspy)

## test_api.py

### Test Cases:
1. **test_create_job**
    - Verifies successful job creation through the API
    - Checks if the response status code is 200
    - Ensures the response JSON contains the correct job ID

2. **test_create_job_error**
    - Tests error handling when job creation fails
    - Verifies that a 500 error is returned with the correct error message

3. **test_stop_job**
    - Tests successful job stopping through the API
    - Checks if the response status code is 200
    - Ensures the response JSON indicates the job was stopped

4. **test_stop_job_not_found**
    - Tests error handling when stopping a non-existent job
    - Verifies that a 404 error is returned with the correct error message

5. **test_get_status**
    - Tests retrieval of scheduler status through the API
    - Checks if the response status code is 200
    - Ensures the response JSON contains the correct status information

## test_cluster_manager.py

### Test Cases:
1. **test_init**
    - Verifies correct initialization of ClusterManager
    - Checks if all attributes are set correctly

2. **test_get_mig_name**
    - Tests the _get_mig_name method
    - Ensures correct MIG name generation based on cluster and region

3. **test_scale_all_regions**
    - Tests the scale_all_regions method
    - Verifies that _scale_region is called for each region with correct parameters

4. **test_scale_region**
    - Tests the _scale_region method
    - Checks if the new target size is calculated correctly
    - Verifies that MigManager is called to scale the MIG

5. **test_get_status**
    - Tests the get_status method
    - Ensures correct retrieval of status for all regions in the cluster

## test_cluster_orchestrator.py

### Test Cases:
1. **test_init**
    - Verifies correct initialization of ClusterOrchestrator
    - Checks if all attributes are set correctly

2. **test_update_status**
    - Tests the update_status method
    - Ensures correct aggregation of status information from all cluster managers

3. **test_scale_clusters**
    - Tests the scale_clusters method
    - Verifies that scale_all_regions is called on each cluster manager with correct parameters

4. **test_scale_clusters_with_missing_data**
    - Tests the scale_clusters method with missing data for some clusters
    - Ensures proper handling of cases where some clusters have no pending or running jobs data

## test_config_manager.py

### Test Cases:
1. **test_load**
    - Tests the load method of ConfigManager
    - Verifies correct loading of configuration from YAML files and environment variables
    - Checks proper overriding behavior

2. **test_getattr**
    - Tests the __getattr__ method of ConfigManager
    - Ensures configuration values can be accessed as attributes of the ConfigManager instance

3. **test_database_url**
    - Tests the construction of the database URL
    - Verifies correct assembly of the database_url property from individual database configuration parameters

4. **test_is_truthy**
    - Tests the is_truthy function
    - Verifies correct identification of truthy values

5. **test_is_falsy**
    - Tests the is_falsy function
    - Verifies correct identification of falsy values

## test_fake_mig_manager.py

### Test Cases:
1. **test_init**
    - Tests the initialization of FakeMigManager
    - Checks if all attributes are set correctly

2. **test_start_stop**
    - Tests the start and stop methods of FakeMigManager
    - Verifies correct setting of the running flag

3. **test_handle_new_job**
    - Tests the handle_new_job method
    - Ensures jobs are correctly added to the job queue

4. **test_scale_mig**
    - Tests the scale_mig method
    - Verifies correct scaling up and down of MIGs

5. **test_assign_job_to_vm**
    - Tests the assign_job_to_vm method
    - Checks if jobs are correctly assigned to VMs and if scaling is performed when needed

6. **test_get_region_for_cluster**
    - Tests the get_region_for_cluster method
    - Ensures correct region selection for a given cluster

7. **test_get_target_and_running_vm_counts**
    - Tests the get_target_and_running_vm_counts method
    - Verifies correct counting of target and running VMs

8. **test_simulate_vm**
    - Tests the simulate_vm method
    - Ensures correct simulation of VM lifecycle, including heartbeats and deletion

9. **test_send_heartbeat**
    - Tests the send_heartbeat method
    - Verifies correct heartbeat message publishing

10. **test_delete_vm**
    - Tests the delete_vm method
    - Ensures correct removal of VMs from MIGs and active_vms list

## test_scheduler.py

### Test Cases:
1. **test_init**
    - Tests the initialization of Scheduler
    - Checks if all attributes are set correctly

2. **test_start_stop**
    - Tests the start and stop methods of Scheduler
    - Verifies correct setting of the running flag and calling of necessary methods

3. **test_add_job**
    - Tests the add_job method
    - Ensures correct job addition to the database

4. **test_schedule_jobs**
    - Tests the _schedule_jobs method
    - Verifies correct job scheduling and status updates

5. **test_listen_for_heartbeats**
    - Tests the _listen_for_heartbeats method
    - Ensures correct handling of heartbeat messages and job status updates

6. **test_monitor_and_scale_clusters**
    - Tests the _monitor_and_scale_clusters method
    - Verifies correct monitoring and scaling of clusters

7. **test_stop_job**
    - Tests the stop_job method
    - Ensures correct stopping of running jobs and handling of non-existent jobs

8. **test_get_status**
    - Tests the get_status method
    - Verifies correct retrieval of overall scheduler status

## test_utils.py

### Test Cases:
1. **test_is_new_job_status_valid**
    - Tests the is_new_job_status_valid function
    - Verifies correct validation of job status transitions

2. **test_get_region_from_vm_name**
    - Tests the get_region_from_vm_name function
    - Ensures correct extraction of region from VM names

3. **test_heartbeat_ordered_job_statuses**
    - Tests the HEARTBEAT_ORDERED_JOB_STATUSES constant
    - Verifies correct order of job statuses