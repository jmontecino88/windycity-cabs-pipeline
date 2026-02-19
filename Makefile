PYTHONPATH=src
export PYTHONPATH

up:
	docker compose up -d

down:
	docker compose down

ingest:
	python -m windycity_cabs.ingest_raw

stage:
	python -m windycity_cabs.stage_trips

load:
	python -m windycity_cabs.load_mysql

transform:
	python -m windycity_cabs.build_marts

dq:
	python -m windycity_cabs.run_dq

export:
	python -m windycity_cabs.export_bi

run:
	make ingest && make stage && make load && make transform && make dq && make export