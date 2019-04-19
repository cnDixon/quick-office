# Create your views here.

from django.db import connection
from django.http import HttpResponse

from schedule import tools
from utils import time_tools


def get_user(request):
    user_id = request.GET.get('user_id', None)
    if not user_id:
        return HttpResponse(tools.result(1, 'error user id.'))

    sql = "SELECT `name`, `user_name` FROM `api_service`.`auth_users` WHERE user_id = '{}'".format(user_id)
    with connection.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchone()

    if not res:
        return HttpResponse(tools.result(1, msg='error user id.'))

    return HttpResponse(tools.result(data={'name': res[0], 'user_name': res[1]}))


def job_status(request):
    date = request.GET.get('date', time_tools.get_date_str())
    sql = """
          SELECT 
            '{start_date}', job_status, etl_system, COUNT(1), SUM(TIMESTAMPDIFF(SECOND,start_time,end_time)) 
          FROM `dmp_schedule`.`etl_job_logs` 
          WHERE 
            start_time BETWEEN '{start_date}' AND '{end_date}' 
          GROUP BY job_status, etl_system""".format(
        start_date=date,
        end_date=time_tools.get_date_str(time_delta=1, appoint_date=time_tools.date_str_parser(date))
    )
    with connection.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchall()
    res = [
        {
            'date': metadata[0], 'status': metadata[1], 'etl_sys': metadata[2], 'ct': metadata[3],
            'used_time': int(metadata[4])
        }
        for metadata in res
    ]
    return HttpResponse(tools.result(data=res))


def run_job(request):
    etl_sys = request.GET.get('etl_sys', None)
    etl_job = request.GET.get('etl_job', None)
    tx_dt = request.GET.get('tx_dt', None)

    if etl_sys and etl_job and tx_dt and time_tools.date_str_parser(tx_dt):
        res = tools.result()
    else:
        res = tools.result(1, msg='params error.')

    return HttpResponse(tools.result(data=res))


def job_info(request):
    etl_sys = request.GET.get('etl_sys', None)
    etl_job = request.GET.get('etl_job', None)

    if not etl_sys or not etl_job:
        return HttpResponse(tools.result(1, msg='params error.'))

    sql = """
    SELECT
      etl_job.etl_system,
      etl_job.etl_job,
      etl_job.cycle,
      etl_job.`enable`,
      etl_job.priority,
      etl_job.etl_group,
      etl_job.last_jobid,
      etl_job.last_jobstatus,
      etl_job.last_txdate,
      etl_job.last_excutor,
      etl_job.last_starttime,
      etl_job.last_endtime,
      etl_job.discription,
      etl_job.respon_user,
      (
      SELECT
        GROUP_CONCAT( 'step_id: ', setp_id, '   script: ', script, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_job_steps 
      WHERE
        etl_job.etl_system = etl_job_steps.etl_system 
        AND etl_job.etl_job = etl_job_steps.etl_job 
      ) scripts,
      (
      SELECT
        GROUP_CONCAT(
         'stream_sys: ', stream_system, '   stream_job: ', stream_job, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_job_stream 
      WHERE
        etl_job.etl_system = etl_job_stream.etl_system 
        AND etl_job.etl_job = etl_job_stream.etl_job 
      ) streamed,
      (
      SELECT
        GROUP_CONCAT(
         'stream_by_sys: ', etl_system, '   stream_by_job: ', etl_job, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_job_stream 
      WHERE
        etl_job.etl_system = etl_job_stream.stream_system 
        AND etl_job.etl_job = etl_job_stream.stream_job 
      ) stream_by,
      (
      SELECT
        GROUP_CONCAT( 'sys: ', dependency_system, '   job: ', dependency_job, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_job_dependency 
      WHERE
        etl_job_dependency.etl_system = etl_job.etl_system 
        AND etl_job_dependency.etl_job = etl_job.etl_job 
      ) dependencies,
      (
      SELECT
        GROUP_CONCAT( 'sys: ', etl_system, '   job: ', etl_job, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_job_dependency 
      WHERE
        etl_job_dependency.dependency_system = etl_job.etl_system 
        AND etl_job_dependency.dependency_job = etl_job.etl_job 
      ) dependency_by,
      (
      SELECT
        GROUP_CONCAT(
         'date_offset: ', date_offset, '   trigger_offset: ', trigger_offset, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_time_trigger 
      WHERE
        etl_time_trigger.etl_system = etl_job.etl_system 
        AND etl_time_trigger.etl_job = etl_job.etl_job 
      ) trigger_info,
      (
      SELECT
        GROUP_CONCAT(
         'window_type: ', window_type, '   date_type: ', date_type, '   start_window: ', start_window, 
         '   end_window: ', end_window, '   enable: ', `enable` SEPARATOR '\n') 
      FROM
        dmp_schedule.etl_time_window 
      WHERE
        etl_time_window.etl_system = etl_job.etl_system 
        AND etl_time_window.etl_job = etl_job.etl_job 
      ) window_info 
    FROM
      ( SELECT * FROM dmp_schedule.etl_job WHERE etl_system = '{etl_sys}' AND etl_job = '{etl_job}' ) etl_job
    """.format(etl_sys=etl_sys, etl_job=etl_job)

    with connection.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchone()

    if not res:
        return HttpResponse(tools.result(1, msg='job not found.'))

    (etl_sys, etl_job, cycle, enable, priority, etl_group, last_job_id, last_job_status, last_tx_date, last_executor,
     last_start_time, last_end_time, description, user, scripts, streamed, stream_by, dependencies, dependency_by,
     trigger_info, window_info) = res

    res = {
        'etl_sys': etl_sys, 'etl_job': etl_job, 'cycle': cycle, 'enable': enable, 'priority': priority,
        'etl_group': etl_group, 'last_job_id': last_job_id, 'last_job_status': last_job_status,
        'last_tx_date': last_tx_date, 'last_executor': last_executor,
        'last_start_time': last_start_time.strftime('%Y-%m-%d'),
        'last_end_time': last_end_time.strftime('%Y%m%d'), 'description': description, 'user': user, 'scripts': scripts,
        'streamed': streamed, 'stream_by': stream_by, 'dependencies': dependencies, 'dependency_by': dependency_by,
        'trigger_info': trigger_info, 'window_info': window_info
    }

    return HttpResponse(tools.result(data=res))
