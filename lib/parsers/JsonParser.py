import pytz
from datetime import datetime
import time
from lib.models.models import Task

time_to_seconds_dict = {
    'second': 1, 'minute': 60, 'hour': 3600,
    'day': 86400
}


def get_execution_time_from_date(timestamp, tz):
    local = pytz.timezone(tz)
    naive = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    local_dt = local.localize(naive, is_dst=None)
    return local_dt.astimezone(pytz.utc)


def get_execution_time_repeated(repeat_type, repeat):
    type_seconds = time_to_seconds_dict.get(repeat_type, None)
    if type_seconds:
        exec_time = time.time() + (type_seconds * repeat)
        return exec_time
    else:
        return None


def parse_task_from_json(task_json):
    t = Task()
    try:
        data = task_json['data']
        task_type = task_json['type']
    except KeyError:
        return {'error':'Missing required param'}

    t.data = data
    t.type = task_type

    timezone = task_json.get('timezone', None)
    timestamp = task_json.get('timestamp', None)
    now = task_json.get('now', False)
    repeat = task_json.get('repeat', None)
    repeat_type = task_json.get('repeat_type', None)

    t.repeat_type = repeat_type
    t.repeat = repeat

    if now:
        exec_time = time.time()

    elif repeat_type:
        exec_time = get_execution_time_repeated(repeat_type, repeat)

    else:
        exec_time = get_execution_time_from_date(timezone, timestamp)

    t.execution_time = exec_time

    return t
