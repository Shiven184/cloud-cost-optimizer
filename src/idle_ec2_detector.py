"""
idle_ec2_detector.py
Finds running EC2 instances with avg CPU below threshold and alerts via SNS.
Triggered: Daily at 9 AM IST (3:30 AM UTC) via EventBridge.
"""

import os
import logging
import boto3
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2_client = boto3.client('ec2')
cw_client  = boto3.client('cloudwatch')
sns_client = boto3.client('sns')

SNS_TOPIC_ARN     = os.environ['SNS_TOPIC_ARN']
CPU_THRESHOLD     = float(os.environ.get('CPU_THRESHOLD', '5'))
AUTO_STOP_ENABLED = os.environ.get('AUTO_STOP_ENABLED', 'false').lower() == 'true'


def get_avg_cpu(instance_id, hours=24):
    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    response   = cw_client.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='CPUUtilization',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average'],
        Unit='Percent'
    )
    points = response.get('Datapoints', [])
    if not points:
        return -1.0
    return round(sum(p['Average'] for p in points) / len(points), 2)


def get_name(tags):
    for tag in (tags or []):
        if tag['Key'] == 'Name':
            return tag['Value']
    return 'unnamed'


def handler(event, context):
    logger.info("Starting idle EC2 detection. Threshold: %.1f%%", CPU_THRESHOLD)

    pages = ec2_client.get_paginator('describe_instances').paginate(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
    )
    instances = [i for p in pages for r in p['Reservations'] for i in r['Instances']]
    logger.info("Found %d running instances", len(instances))

    idle = []
    for inst in instances:
        age_hours = (datetime.now(timezone.utc) - inst['LaunchTime']).total_seconds() / 3600
        if age_hours < 2:
            continue
        avg_cpu = get_avg_cpu(inst['InstanceId'])
        if 0 <= avg_cpu < CPU_THRESHOLD:
            idle.append({
                'instance_id':   inst['InstanceId'],
                'name':          get_name(inst.get('Tags', [])),
                'instance_type': inst['InstanceType'],
                'avg_cpu':       avg_cpu,
                'az':            inst['Placement']['AvailabilityZone'],
                'age_hours':     round(age_hours, 1)
            })

    stopped = []
    if AUTO_STOP_ENABLED and idle:
        ec2_client.stop_instances(InstanceIds=[i['instance_id'] for i in idle])
        stopped = [i['instance_id'] for i in idle]

    if idle:
        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        lines = [
            "AWS COST OPTIMIZER - Idle EC2 Report",
            f"Scan time     : {ist_now.strftime('%d %B %Y, %I:%M %p IST')}",
            f"CPU threshold : {CPU_THRESHOLD}% (24hr average)",
            f"Auto-stop     : {'ENABLED' if AUTO_STOP_ENABLED else 'DISABLED'}",
            f"Idle instances: {len(idle)}",
            "",
            "IDLE INSTANCES",
        ]
        for i in idle:
            status = "STOPPED" if i['instance_id'] in stopped else "RUNNING - action needed"
            lines += [
                f"  ID    : {i['instance_id']}",
                f"  Name  : {i['name']}",
                f"  Type  : {i['instance_type']}  |  AZ: {i['az']}",
                f"  CPU   : {i['avg_cpu']}% avg over 24 hours",
                f"  Age   : {i['age_hours']} hours",
                f"  Status: {status}",
                ""
            ]
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[AWS Cost Alert] {len(idle)} idle EC2 instance(s) detected",
            Message="\n".join(lines)
        )
        logger.info("Alert sent for %d idle instances", len(idle))
    else:
        logger.info("No idle instances found")

    return {'statusCode': 200, 'total_running': len(instances), 'idle_count': len(idle)}
