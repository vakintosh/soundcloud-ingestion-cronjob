# soundcloud-ingestion-cronjob

A cloud-native service designed to reliably poll external Soundcloud RSS feeds, ensure data integrity via idempotent checks, and emit clean, structured JSON events for further processing. Built for high reliability using Kubernetes CronJob scheduling and persistent storage.