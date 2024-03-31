import re, json, datetime, time
from datetime import datetime, timedelta
from dateutil import parser, tz

def extract_alert_info(alert_info):

    if isinstance(alert_info, str):

        alert_info = alert_info.replace("\'", '\"')
        alert_dict = alert_info.replace("'", '"')

        alert_dict = re.sub(r'"groupKey": ".*?",', '', alert_dict)
        alert_dict = json.loads(alert_dict)
    else:
        alert_dict = alert_info

    alert_status = alert_dict['status']
    alert_status = alert_status.strip()

    # identify the first alert
    alert_desc = alert_dict['alerts'][0]['annotations']['description']
    alert_desc = alert_desc.strip()

    alert_exporter = alert_dict['alerts'][0]['labels']['instance']  # IP:pport
    alert_exporter = alert_exporter.strip()

    # Critical High Warning Info
    alert_level = alert_dict['alerts'][0]['labels']['severity']
    alert_level = alert_level.strip()

    alert_name = alert_dict['alerts'][0]['labels']['alertname']

    starts_at = parser.parse(alert_dict['alerts'][0]['startsAt'])
    ends_at = parser.parse(alert_dict['alerts'][0]['endsAt'])

    if ends_at.year == 1:
        ends_at = starts_at + timedelta(minutes=2)

    # Convert the start and end times to seconds since the Unix epoch
    epoch = datetime(1970, 1, 1, tzinfo=tz.tzutc())  # set timezone to UTC
    starts_at_seconds = (starts_at - epoch).total_seconds()
    ends_at_seconds = (ends_at - epoch).total_seconds()

    start_date = time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(starts_at_seconds))
    end_date = time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(ends_at_seconds))

    starts_at_seconds = str(int(starts_at_seconds))
    ends_at_seconds = str(int(ends_at_seconds))

    alert_info = f"Alert Starts At: {start_date}\nAlert Ends At: {end_date}\nAlert Status: {alert_status}\nAlert Description: {alert_desc}\nAlert Level: {alert_level}"

    alert_dict = {"alert_name": alert_name,
                  "alert_status": alert_status,
                  "alert_level": alert_level,
                  "alert_desc": alert_desc,
                  "alert_exporter": alert_exporter,
                  "start_time": starts_at_seconds,
                  "end_time": ends_at_seconds}

    return alert_info, alert_dict