import boto3
from botocore.exceptions import ClientError

def get_enabled_regions():
    """Returns a list of regions that are enabled in the account."""
    ec2 = boto3.client('ec2', region_name='us-east-1')
    regions = ec2.describe_regions(AllRegions=False)  # False only shows enabled regions
    return [r['RegionName'] for r in regions['Regions']]

def enable_guardduty_features(region):
    """Enables GuardDuty and configures S3/EKS protection."""
    gd = boto3.client('guardduty', region_name=region)
    
    # 1. Check for existing detector
    detectors = gd.list_detectors()
    
    if detectors['DetectorIds']:
        detector_id = detectors['DetectorIds'][0]
        action = "Updated"
    else:
        # 2. Create detector if it doesn't exist
        response = gd.create_detector(Enable=True)
        detector_id = response['DetectorId']
        action = "Enabled"

    # 3. Enable S3 and EKS Protection
    # We use update_detector to ensure specific data sources are ON
    gd.update_detector(
        DetectorId=detector_id,
        DataSources={
            'S3Logs': {'Enable': True},
            'Kubernetes': {'AuditLogs': {'Enable': True}}
        },
        # Enabling modern 'Features' structure for EKS Runtime monitoring if available
        Features=[
            {'Name': 'S3_DATA_EVENTS', 'Status': 'ENABLED'},
            {'Name': 'EKS_AUDIT_LOGS', 'Status': 'ENABLED'},
            {'Name': 'EKS_RUNTIME_MONITORING', 'Status': 'ENABLED', 
             'AdditionalConfiguration': [{'Name': 'EKS_ADDON_MANAGEMENT', 'Status': 'ENABLED'}]}
        ]
    )
    return action

def main():
    print("--- AWS GuardDuty Deployment Plan ---")
    try:
        regions = get_enabled_regions()
        print(f"Detected {len(regions)} enabled regions.\n")
    except Exception as e:
        print(f"Error fetching regions: {e}")
        return

    summary = []

    for region in regions:
        try:
            status = enable_guardduty_features(region)
            summary.append({"Region": region, "Status": "Success", "Action": status})
        except ClientError as e:
            summary.append({"Region": region, "Status": "Failed", "Error": str(e)})
        except Exception as e:
            summary.append({"Region": region, "Status": "Error", "Error": "Not supported in region"})

    # Print Summary Table
    print(f"{'Region':<20} | {'Status':<10} | {'Action/Note'}")
    print("-" * 50)
    for entry in summary:
        msg = entry.get('Action') if entry['Status'] == 'Success' else entry.get('Error')[:30] + "..."
        print(f"{entry['Region']:<20} | {entry['Status']:<10} | {msg}")

if __name__ == "__main__":
    main()
