# CLIN Pipelines Dags

## Airflow dev stack

Create and customize `.env` file :

```
cp .env.sample .env
```

Deploy stack :

```
docker-compose up
```

Web UI credentials :

- Username : `airflow`
- Password : `airflow`

Create Airflow variables (in the UI => Admin => Variables):

- environment : `qa`
- kubernetes_namespace : `cqgc-qa`
- kubernetes_context_default : `kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc`
- kubernetes_context_etl : `kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc`

Get scheduler container shell :

```
docker-compose exec airflow-scheduler bash
```

Execute task

```
airflow tasks test <dag> <task> 2022-01-01
```

# Troubleshooting

## Failed to establish a new connection: [Errno 110] Connection timed out

Can be a host <=> ip resolution issue in local. Add to your `/etc/hosts` file the following:

```
10.128.81.22  k8-api.etl.cqgc.hsj.rtss.qc.ca
10.128.81.202 k8-api.qa.cqgc.hsj.rtss.qc.ca
```

