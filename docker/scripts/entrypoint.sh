#!/usr/bin/env bash
airflow initdb
airflow webserver
airflow connections --add --conn_id 'clin_object_store' --conn_login 'minio' --conn_password 'minio123' --conn_type 's3' --conn_extra '{"host":"http://192.168.0.16:9000"}'