
import os
from functools import reduce

import django
import pandas as pd

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "eba.settings")

django.setup()

from django.db import connection as con # isort:skip # noqa

from eba.user_data.models import DataFormatInstance, DataTypeSetting, Organization # isort:skip # noqa

def get_clicks(search_start_date,search_end_date):
    print("fetching click data")
    cursor_params = {
        "search_start_date": search_start_date,
        "search_end_date": search_end_date,
    }

    q = """
            SELECT e.employee_obj_id, COUNT(access_action_obj_id) as clicks_count
            FROM user_data_evidence e
            WHERE
                e.time::date BETWEEN %(search_start_date)s and %(search_end_date)s
            GROUP BY e.employee_obj_id
        """

    return pd.read_sql_query(q, con, params=cursor_params)


def get_patients_accessed(search_start_date,search_end_date):
    print("fetching patients accessed data")
    cursor_params = {
        "search_start_date": search_start_date,
        "search_end_date": search_end_date,
    }

    q = """
            SELECT a.employee_obj_id, count(distinct a.patient_obj_id) as patients_accessed_count
            FROM user_data_firstaccess a
            WHERE
                a.time::date BETWEEN %(search_start_date)s and %(search_end_date)s
            GROUP BY a.employee_obj_id

        """

    return pd.read_sql_query(q, con, params=cursor_params)


def get_after_hours_access(search_start_date, search_end_date, start_time, end_time):
    print("fetching after hours data")
    cursor_params = {
        "search_start_date": search_start_date,
        "search_end_date": search_end_date,
        "start_time": start_time,
        "end_time": end_time,
    }

    q = """
            SELECT a.employee_obj_id, count(distinct a.patient_obj_id) as after_hours_count
            FROM user_data_evidence a
            WHERE
                a.time::date BETWEEN %(search_start_date)s AND %(search_end_date)s
                AND a.time::time NOT BETWEEN %(start_time)s and %(end_time)s
                AND a.access_action_obj_id IS NOT NULL
            GROUP BY a.employee_obj_id
        """

    return pd.read_sql_query(q, con, params=cursor_params)


def get_dept(search_start_date,search_end_date):
    print("fetching department")

    cursor_params = {"search_start_date": search_start_date,"search_end_date": search_end_date}

    q = """
        SELECT DISTINCT ON (employee_obj_id)
        e.employee_obj_id, f.value
        FROM user_data_firstaccess e, user_data_employeedepartmentfieldvalue d, user_data_fieldvalue f
        WHERE
            e.employee_obj_id = d.person_obj_id
            AND d.value_id = f.id
            AND e.time::date BETWEEN %(search_start_date)s and %(search_end_date)s
        """

    return pd.read_sql_query(q, con, params=cursor_params)

def get_employee_term(term_search_date,org):
    print("fetching indicator")
    
    hire_dfi = DataTypeSetting.objects.get_dfi_by_name(org, DataFormatInstance.HIRED)
    cursor_params = {
        'term_dfi_id': DataFormatInstance.objects.get(org=308, name='term date').id,
        'hire_dfi_id': hire_dfi.id,
        'term_search_date': term_search_date
    }
    
    q = """
   	with term_employee_value as (
        select distinct on (employee_obj_id) employee_obj_id, value
        from user_data_employeeinfo te
        inner join user_data_fieldvalue term_value
            on te.value_id = term_value.id
            and term_value.data_format_instance_id = 141
        where te.time > '2020-05-08'
        order by employee_obj_id, time desc
    	)
    	, hire_employee_value as (
        select distinct on (employee_obj_id) employee_obj_id, value
        from user_data_employeeinfo he
        left join user_data_fieldvalue hired_value
            on he.value_id = hired_value.id
            and hired_value.data_format_instance_id = 16
        	where he.time > '2020-05-08'
        	order by employee_obj_id, time desc
    	)
    	select  t.employee_obj_id,
            CASE
                WHEN t.value > h.value
                THEN '1'
                ELSE '0'
            END AS indicator
    	from term_employee_value t
    	Left JOIN hire_employee_value h on h.employee_obj_id = t.employee_obj_id;
	 """
    
    return pd.read_sql_query(q, con, params=cursor_params)


def main():
    org = Organization.objects.filter(name__icontains='Maize')[0]
    
    start_time = "08:00"
    end_time = "18:00"

    search_start_date = "2020-01-01"
    search_end_date = "2020-06-08"
    term_search_date = '2020-05-08'

    df_clicks = get_clicks(search_start_date,search_end_date)
    print("df_clicks")
    print(df_clicks.head())
    
    df_patients_accessed = get_patients_accessed(search_start_date,search_end_date)
    print("df_patients_accessed")
    print(df_patients_accessed.head())
    
    df_after_hours_access = get_after_hours_access(search_start_date, search_end_date, start_time, end_time)
    print("df_after_hours_access")
    print(df_after_hours_access.head())
    
    df_dept = get_dept(search_start_date,search_end_date)
    print("df_dept")
    print(df_dept.head())
    
    df_terminated = get_employee_term(term_search_date,org)
    print("df_terminated")
    print(df_terminated.head())
    
    dfs = [df_clicks, df_patients_accessed, df_after_hours_access, df_dept, df_terminated]
    #dfs = [df_dept,df_terminated]
    print("joining data")
    df_joined = reduce(
        lambda left, right: pd.merge(left, right, on="employee_obj_id",how="right"), dfs
    )
    print('df_joined')
    print(df_joined.head())
    print(df_joined.dtypes)

    df_joined.to_pickle('/data/home/vagrant/eba/tmp/cone_burnout_data.pkl')
    #df_joined.to_pickle('/data/vagrant/eba/tmp/shadow_burnout_data.pkl')
    #df_joined.to_pickle("/Users/dadams/github/eba/ebard/burnout_data.pkl")


if __name__ == "__main__":
    main()
