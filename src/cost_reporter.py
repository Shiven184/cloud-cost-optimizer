"""
cost_reporter.py
Fetches AWS cost from Cost Explorer and emails a daily summary via SNS.
Triggered: Daily at 8 AM IST (2:30 AM UTC) via EventBridge.
"""

import os
import logging
import boto3
from datetime import datetime, timedelta, date, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ce_client  = boto3.client('ce', region_name='us-east-1')
sns_client = boto3.client('sns')

SNS_TOPIC_ARN    = os.environ['SNS_TOPIC_ARN']
BUDGET_THRESHOLD = float(os.environ.get('BUDGET_THRESHOLD', '50'))


def get_mtd_cost_by_service():
    today     = date.today()
    first_day = today.replace(day=1).strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')
    response  = ce_client.get_cost_and_usage(
        TimePeriod={'Start': first_day, 'End': today_str},
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )
    costs = {}
    for result in response.get('ResultsByTime', []):
        for group in result.get('Groups', []):
            amount = float(group['Metrics']['UnblendedCost']['Amount'])
            if amount > 0.01:
                costs[group['Keys'][0]] = round(amount, 4)
    return dict(sorted(costs.items(), key=lambda x: x[1], reverse=True))


def get_daily_costs(days=7):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days)
    response   = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date.strftime('%Y-%m-%d'),
            'End':   end_date.strftime('%Y-%m-%d')
        },
        Granularity='DAILY',
        Metrics=['UnblendedCost']
    )
    return [
        {
            'date': r['TimePeriod']['Start'],
            'cost': round(float(r['Total']['UnblendedCost']['Amount']), 4)
        }
        for r in response.get('ResultsByTime', [])
    ]


def get_mtd_total():
    today     = date.today()
    first_day = today.replace(day=1).strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')
    response  = ce_client.get_cost_and_usage(
        TimePeriod={'Start': first_day, 'End': today_str},
        Granularity='MONTHLY',
        Metrics=['UnblendedCost']
    )
    total = sum(
        float(r['Total']['UnblendedCost']['Amount'])
        for r in response.get('ResultsByTime', [])
    )
    return round(total, 2)


def handler(event, context):
    logger.info("Starting daily cost report")

    service_costs = get_mtd_cost_by_service()
    daily_costs   = get_daily_costs(7)
    mtd_total     = get_mtd_total()

    budget_pct = round((mtd_total / BUDGET_THRESHOLD) * 100, 1) if BUDGET_THRESHOLD > 0 else 0
    remaining  = round(BUDGET_THRESHOLD - mtd_total, 2)
    status     = "ON TRACK" if mtd_total <= BUDGET_THRESHOLD else "OVER BUDGET"
    yesterday  = daily_costs[-1]['cost'] if daily_costs else 0
    ist_now    = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

    lines = [
        "AWS DAILY COST REPORT",
        f"Generated     : {ist_now.strftime('%d %B %Y, %I:%M %p IST')}",
        "",
        "MONTHLY SUMMARY",
        f"Month-to-date : ${mtd_total}",
        f"Monthly budget: ${BUDGET_THRESHOLD}",
        f"Budget used   : {budget_pct}%",
        f"Remaining     : ${remaining}",
        f"Status        : {status}",
        "",
        f"YESTERDAY     : ${yesterday}",
        "",
        "LAST 7 DAYS",
    ]

    max_cost = max((d['cost'] for d in daily_costs), default=1) or 1
    for day in daily_costs:
        bar = "=" * int((day['cost'] / max_cost) * 20)
        lines.append(f"  {day['date']}  ${day['cost']:>7.4f}  {bar}")

    lines += ["", "TOP SERVICES THIS MONTH"]
    for service, cost in list(service_costs.items())[:8]:
        pct = round((cost / mtd_total) * 100, 1) if mtd_total > 0 else 0
        lines.append(f"  {service[:40].ljust(40)}  ${cost:>8.4f}  ({pct}%)")

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[AWS Cost Report] ${mtd_total} MTD | Budget {budget_pct}% used | {status}",
        Message="\n".join(lines)
    )

    logger.info("Cost report sent. MTD: $%s", mtd_total)
    return {'statusCode': 200, 'mtd_cost': mtd_total}
