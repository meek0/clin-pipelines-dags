# CLIN Pipelines Dags

## Airflow dev stack

Create and customize `.env` file :

```
cp .env.sample .env
```

Deploy stack :

```
docker compose up
```

Web UI credentials :

- Username : `airflow`

- Password : `airflow`

Get scheduler container shell :

```
docker compose exec airflow-scheduler bash
```

Execute task

```
airflow tasks test <dag> <task> 2022-01-01
```
