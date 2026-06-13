"""
unattached_volume_finder.py
Finds EBS volumes not attached to any EC2 and reports monthly waste via SNS.
Triggered: Every Monday at 9 AM IST (3:30 AM UTC) via EventBridge.
"""

import os
import logging
import boto3
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2_client = boto3.client('ec2')
sns_client = boto3.client('sns')

SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

PRICE_PER_GB = {
    'gp3': 0.08, 'gp2': 0.10, 'io1': 0.125,
    'io2': 0.125, 'st1': 0.045, 'sc1': 0.025, 'standard': 0.05
}


def handler(event, context):
    logger.info("Starting unattached EBS volume scan")

    pages = ec2_client.get_paginator('describe_volumes').paginate(
        Filters=[{'Name': 'status', 'Values': ['available']}]
    )

    volumes = []
    for page in pages:
        for vol in page['Volumes']:
            price   = PRICE_PER_GB.get(vol.get('VolumeType', 'gp2'), 0.10)
            cost    = round(vol.get('Size', 0) * price, 4)
            created = vol.get('CreateTime')
            days    = (datetime.now(timezone.utc) - created).days if created else 0
            name    = next((t['Value'] for t in vol.get('Tags', []) if t['Key'] == 'Name'), 'unnamed')

            volumes.append({
                'volume_id':      vol['VolumeId'],
                'name':           name,
                'volume_type':    vol.get('VolumeType', 'unknown'),
                'size_gb':        vol.get('Size', 0),
                'az':             vol.get('AvailabilityZone', 'unknown'),
                'age_days':       days,
                'cost_per_month': cost,
                'wasted_cost':    round(cost * (days / 30), 4)
            })

    volumes.sort(key=lambda x: x['cost_per_month'], reverse=True)
    total_waste = round(sum(v['cost_per_month'] for v in volumes), 2)
    total_gb    = sum(v['size_gb'] for v in volumes)

    logger.info("Found %d unattached volumes. Monthly waste: $%s", len(volumes), total_waste)

    if not volumes:
        logger.info("No unattached volumes found")
        return {'statusCode': 200, 'unattached_count': 0}

    ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    lines = [
        "AWS COST OPTIMIZER - Unattached EBS Volumes",
        f"Scan time   : {ist_now.strftime('%d %B %Y, %I:%M %p IST')}",
        f"Volumes     : {len(volumes)}",
        f"Monthly cost: ${total_waste}",
        f"Annual cost : ${round(total_waste * 12, 2)}",
        f"Total size  : {total_gb} GB",
        "",
        "UNATTACHED VOLUMES",
    ]
    for v in volumes:
        lines += [
            f"  Volume ID : {v['volume_id']}",
            f"  Name      : {v['name']}",
            f"  Type/Size : {v['volume_type']} / {v['size_gb']} GB  |  AZ: {v['az']}",
            f"  Age       : {v['age_days']} days unattached",
            f"  Cost      : ${v['cost_per_month']}/month  (wasted: ${v['wasted_cost']})",
            ""
        ]
    lines += [
        "TO FIX",
        "  1. Snapshot: aws ec2 create-snapshot --volume-id <id> --description backup",
        "  2. Delete  : aws ec2 delete-volume --volume-id <id>",
    ]

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[AWS Cost Alert] {len(volumes)} unattached EBS volumes wasting ${total_waste}/month",
        Message="\n".join(lines)
    )
    logger.info("EBS waste alert sent")
    return {'statusCode': 200, 'unattached_count': len(volumes), 'monthly_waste': total_waste}
