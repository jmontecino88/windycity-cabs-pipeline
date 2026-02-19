# WindyCity Cabs Pipeline Scaffold

## Objective
Set up a minimal local project scaffold for the WindyCity Cabs data pipeline exercise, without implementing ingestion or transformations yet.

## Stack
- Python
- MySQL 8 with Docker Compose
- Local execution

## Windows usage
```powershell
cd "C:\Proyectos IA\windycity-cabs-pipeline"
copy .env.example .env
docker compose up -d
python run.py ingest
```