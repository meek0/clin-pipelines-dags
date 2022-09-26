# CLIN Pipelines Dags

## Python virtual environment

Create venv :

```
python -m venv venv
```

Activate venv :

```
source venv/bin/activate
```

Install requirements :

```
pip install -r requirements
```

## Airflow dev stack

Create `.env` file :

```
cp .env.sample .env
```

Deploy stack :

```
docker-compose up
```

Login to Airflow UI :

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

Create Airflow variables (Airflow UI => Admin => Variables) :

- environment : `qa`
- kubernetes_namespace : `cqgc-qa`
- kubernetes_context_default : `kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc`
- kubernetes_context_etl : `kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc`
- s3_conn_id : `minio`
- show_test_dags : `yes`

Test one task :

```
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```

## MinIO

Login to MinIO console :

- URL : `http://localhost:59001`
- Username : `minioadmin`
- Password : `minioadmin`

Create Airflow connection (Airflow UI => Admin => Connections) :

- Connection Id : `minio`
- Connection Type : `Amazon S3`
- Extra :
```
{
    "host": "http://minio:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin"
}
```

## Troubleshooting

### Failed to establish a new connection: [Errno 110] Connection timed out

Can be a host <=> ip resolution issue in local. Add to your `/etc/hosts` file the following :

```
10.128.81.22  k8-api.etl.cqgc.hsj.rtss.qc.ca
10.128.81.202 k8-api.qa.cqgc.hsj.rtss.qc.ca
```
