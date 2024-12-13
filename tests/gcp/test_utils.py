from app.gcp.utils import (
    get_region_from_vm_name,
    get_mig_name_from_vm_name,
    get_mig_name_from_cluster_and_region
)

def test_get_region_from_vm_name():
    """Test extracting region from VM names."""
    # Test standard VM name format
    vm_name = "pipeline-zen-jobs-4xa100-40gb-us-central1-vm-0001"
    assert get_region_from_vm_name(vm_name) == "us-central1"

    # Test different region formats
    cases = [
        ("pipeline-zen-jobs-4xa100-40gb-europe-west4-vm-0001", "europe-west4"),
        ("pipeline-zen-jobs-4xa100-40gb-asia-east1-vm-0001", "asia-east1"),
        ("pipeline-zen-jobs-4xa100-40gb-me-west1-vm-0001", "me-west1"),
        ("pipeline-zen-jobs-4xa100-40gb-us-west2-vm-0001", "us-west2"),
    ]
    for vm_name, expected_region in cases:
        assert get_region_from_vm_name(vm_name) == expected_region

    # Test handling of None input
    assert get_region_from_vm_name(None) is None
    assert get_region_from_vm_name("") is None

def test_get_mig_name_from_vm_name():
    """Test extracting MIG name from VM names."""
    # Test standard VM name format
    vm_name = "pipeline-zen-jobs-4xa100-40gb-us-central1-vm-0001"
    expected = "pipeline-zen-jobs-4xa100-40gb-us-central1-mig"
    assert get_mig_name_from_vm_name(vm_name) == expected

    # Test different cluster and region combinations
    cases = [
        (
            "pipeline-zen-jobs-8xv100-europe-west4-vm-0002",
            "pipeline-zen-jobs-8xv100-europe-west4-mig"
        ),
        (
            "pipeline-zen-jobs-1xa100-80gb-asia-east1-vm-0003",
            "pipeline-zen-jobs-1xa100-80gb-asia-east1-mig"
        ),
        (
            "pipeline-zen-jobs-2xa100-40gb-me-west1-vm-0004",
            "pipeline-zen-jobs-2xa100-40gb-me-west1-mig"
        ),
    ]
    for vm_name, expected_mig_name in cases:
        assert get_mig_name_from_vm_name(vm_name) == expected_mig_name

    # Test handling of None input
    assert get_mig_name_from_vm_name(None) is None

    # Test handling of invalid VM names
    # The implementation returns up to the first hyphen for any hyphenated string
    assert get_mig_name_from_vm_name("invalid-vm-name") == "invalid-mig"
    assert get_mig_name_from_vm_name("pipeline-zen-jobs") == "pipeline-mig"
    assert get_mig_name_from_vm_name("pipeline-zen-jobs-") == "pipeline-zen-mig"
    assert get_mig_name_from_vm_name("") is None

def test_get_mig_name_from_cluster_and_region():
    """Test generating MIG name from cluster and region."""
    # Test standard combinations
    cases = [
        ("4xa100-40gb", "us-central1", "pipeline-zen-jobs-4xa100-40gb-us-central1-mig"),
        ("8xv100", "europe-west4", "pipeline-zen-jobs-8xv100-europe-west4-mig"),
        ("1xa100-80gb", "asia-east1", "pipeline-zen-jobs-1xa100-80gb-asia-east1-mig"),
        ("2xa100-40gb", "me-west1", "pipeline-zen-jobs-2xa100-40gb-me-west1-mig"),
    ]
    for cluster, region, expected_name in cases:
        assert get_mig_name_from_cluster_and_region(cluster, region) == expected_name

    # Test with different cluster formats
    cluster_cases = [
        "local",  # Local development cluster
        "test-cluster",  # Test cluster
        "1x-special-gpu",  # Special GPU type
        "custom-cluster-name",  # Custom cluster name
    ]
    region = "us-central1"
    for cluster in cluster_cases:
        expected = f"pipeline-zen-jobs-{cluster}-{region}-mig"
        assert get_mig_name_from_cluster_and_region(cluster, region) == expected

    # Test with various region formats
    region_cases = [
        "local-region1",  # Local development region
        "test-region-1",  # Test region
        "custom-region",  # Custom region name
    ]
    cluster = "4xa100-40gb"
    for region in region_cases:
        expected = f"pipeline-zen-jobs-{cluster}-{region}-mig"
        assert get_mig_name_from_cluster_and_region(cluster, region) == expected

    # Test with empty strings
    assert get_mig_name_from_cluster_and_region("", "") == "pipeline-zen-jobs---mig"

    # Test with None values - the implementation appears to convert None to string "None"
    assert get_mig_name_from_cluster_and_region(None, "us-central1") == "pipeline-zen-jobs-None-us-central1-mig"
    assert get_mig_name_from_cluster_and_region("4xa100-40gb", None) == "pipeline-zen-jobs-4xa100-40gb-None-mig"
    assert get_mig_name_from_cluster_and_region(None, None) == "pipeline-zen-jobs-None-None-mig"
