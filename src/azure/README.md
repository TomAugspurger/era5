# ETL


## Dockerfile

```
az acr build -r pccomponents -g pccomponents-acr_rg --subscription="Planetary Computer" -t pc-etl-task-era5:latest . -f Dockerfile
```