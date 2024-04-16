from sqlalchemy import create_engine
import pandas as pd
from dataBase import DataBase
import os
from loguru import logger
from typing import Dict, List, Set, Any, Union



def get_count_of_data() -> int:
    db = DataBase()
    db.connect()
    data_count = db.count_of_data()
    db.close_connection()
    return data_count

def salary_filter(data_frame) -> pd.DataFrame:
    data_frame = data_frame[(data_frame.salary_from.isna() == False) & (data_frame.salary_to.isna() == False)]
    data_frame = data_frame[data_frame.salary_currency == 'RUR']
    data_frame.loc[data_frame.salary_gross == True, ['salary_to', 'salary_from']] *= 0.87

    data_frame['salary_mean'] = data_frame.loc[:, ['salary_to', 'salary_from']].mean(axis=1)
    data_frame = data_frame[data_frame.salary_mean > 10000]
    return data_frame


def filter_valid_data_from_dataframe(data_frame, values_list):
    data_list = []
    list_name = []
    for i in values_list:
        if i not in data_frame.columns:
            data_list.append(pd.Series())
        else:
            x = data_frame.loc[data_frame[f'{i}'].isna() == False, f'{i}']
            if len(x) >= 10:
                data_list.append(x)
            else:
                data_list.append(pd.Series())
        list_name.append(i)
    return data_list, list_name

def get_dataframe_from_db() -> pd.DataFrame:
    engine = create_engine('postgresql://myuser:mypassword@postgres:5432/mydatabase')
    query = "select (data_jsonb ->> 'id')::int as id, (data_jsonb -> 'area' ->> 'id')::int as area_id, data_jsonb -> 'area' ->> 'name' as area_name, data_jsonb ->> 'code' as code, data_jsonb ->> 'name' as name, (data_jsonb -> 'test' ->> 'required')::bool as test_required, data_jsonb -> 'type' ->> 'id' as type_id, (data_jsonb ->> 'hidden')::bool as hidden, (data_jsonb -> 'salary'->>'to')::int as salary_to, (data_jsonb -> 'salary'->>'from')::int as salary_from, (data_jsonb -> 'salary'->>'gross')::bool as salary_gross, data_jsonb -> 'salary'->>'currency' as salary_currency, (data_jsonb -> 'address' ->> 'lat')::real as address_lat, (data_jsonb -> 'address' ->> 'lng')::real as address_lat, data_jsonb -> 'address' ->> 'raw' as address_raw, data_jsonb -> 'address' ->> 'city' as address_city, (data_jsonb -> 'address' -> 'metro'->>'line_id')::int as metro_line_id, data_jsonb -> 'address' -> 'metro'->>'line_name' as metro_line_name, data_jsonb -> 'address' -> 'metro'->>'station_name' as metro_station_name, (data_jsonb -> 'address' -> 'metro'->>'lat')::real as metro_lat, (data_jsonb -> 'address' -> 'metro'->>'lng')::real as metro_lng, data_jsonb -> 'address' ->> 'street' as address_street, data_jsonb -> 'address' ->> 'building' as address_building, data_jsonb -> 'address' ->> 'description' as address_description, jsonb_array_length(data_jsonb -> 'address' -> 'metro_stations') as count_metro_stations, (select coalesce(array_agg(station->>'station_name')) from jsonb_array_elements(coalesce(data_jsonb -> 'address' -> 'metro_stations', '[]')) as station) as metro_stations, (data_jsonb ->> 'premium')::bool as premium, (data_jsonb ->> 'archived')::bool as archived, (data_jsonb -> 'employer'->>'id')::int as employer_id, data_jsonb -> 'employer'->>'name' as employer_name, (data_jsonb -> 'employer'->>'trusted')::bool as employer_trusted, (data_jsonb -> 'employer'->>'accredited_it_employer')::bool as accredited_it_employer, (data_jsonb ->> 'has_test')::bool as has_test, data_jsonb -> 'schedule' ->>'name' as schedule, (select coalesce(json_object_agg(lang->>'name', lang->'level'->>'id')) from jsonb_array_elements(coalesce(data_jsonb -> 'languages', '[]')) as lang) as languages, (data_jsonb ->> 'created_at')::timestamptz as created_at, (data_jsonb ->> 'published_at')::timestamptz as published_at, (data_jsonb ->> 'initial_created_at')::timestamptz as initial_created_at, data_jsonb -> 'department'->>'name' as department, data_jsonb -> 'employment'->>'name' as employment, data_jsonb -> 'experience'->>'name' as experience, (select coalesce(array_agg(key_skills->>'name')) from jsonb_array_elements(coalesce(data_jsonb -> 'key_skills', '[]')) as key_skills) as key_skills, jsonb_array_length(data_jsonb -> 'key_skills') as count_key_skills, (data_jsonb ->> 'accept_kids')::bool as accept_kids, data_jsonb ->> 'description' as description, data_jsonb -> 'billing_type'->>'name' as billing_type, data_jsonb -> 'working_days'->0->>'name' as working_days, (data_jsonb ->> 'allow_messages')::bool as allow_messages, (data_jsonb ->> 'accept_temporary')::bool as accept_temporary, (data_jsonb ->> 'accept_handicapped')::bool as accept_handicapped, (data_jsonb -> 'professional_roles'->0->>'id')::int as professional_roles_id, data_jsonb -> 'professional_roles'->0->>'name' as professional_roles_name, data_jsonb -> 'working_time_modes'->0->>'name' as working_time_modes, (select coalesce(array_agg(driver_license_types->>'id')) from jsonb_array_elements(coalesce(data_jsonb -> 'driver_license_types', '[]')) as driver_license_types) as driver_license_types, data_jsonb -> 'working_time_intervals'->0->>'name' as working_time_intervals, (data_jsonb ->> 'quick_responses_allowed')::bool as quick_responses_allowed, (data_jsonb ->> 'response_letter_required')::bool as response_letter_required, (data_jsonb ->> 'accept_incomplete_resumes')::bool as accept_incomplete_resumes from vacancies"
    data_frame = pd.read_sql_query(query, engine)
    save_df_to_csv(data_frame)
    return data_frame

def save_df_to_csv(data_frame: pd.DataFrame) -> None:
    data_frame.to_csv('/app/data/dataframe.csv', index=True)

def read_data() -> Union[pd.DataFrame, None]:
    if os.path.exists('/app/data/dataframe.csv'):
        data_frame = pd.read_csv('/app/data/dataframe.csv')
    else:
        data_frame = None
        logger.info("File dataframe.csv does not exist")
    return data_frame
