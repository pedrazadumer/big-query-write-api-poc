# Prueba de Concepto | BigQuery Write API

## Pre-Requisitos

* Para usar la API de Storage Write, se necesita el siguiente permiso de BigQuery: **bigquery.tables.updateData**.

Los roles predefinidos de IAM a continuación tienen el rol requerido:

- bigquery.dataEditor
- bigquery.dataOwner
- bigquery.admin

Para más información: https://cloud.google.com/bigquery/docs/write-api#required_permissions

* Se requiere instalar el cliente `gcloud` (https://cloud.google.com/sdk/docs/install)

* Se requiere la variable de entorno GOOGLE_APPLICATION_CREDENTIALS apuntando a la ruta donde se encuentra el archivo JSON con las credenciales de API de GCP.