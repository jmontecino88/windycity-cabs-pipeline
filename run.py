import os
import subprocess
import sys

COMMANDS = {
    "ingest": "windycity_cabs.ingest_raw",
    "stage": "windycity_cabs.stage_trips",
    "load": "windycity_cabs.load_mysql",
    "transform": "windycity_cabs.build_marts",
    "dq": "windycity_cabs.run_dq",
    "export": "windycity_cabs.export_bi",
}

SEQUENCE = ["ingest", "stage", "load", "transform", "dq", "export"]


def build_env() -> dict:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"src{os.pathsep}{existing}" if existing else "src"
    return env


def run_command(name: str) -> int:
    module = COMMANDS[name]
    result = subprocess.run([sys.executable, "-m", module], env=build_env())
    return result.returncode


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python run.py [ingest|stage|load|transform|dq|export|run]")
        return 1

    command = sys.argv[1]

    if command == "run":
        for step in SEQUENCE:
            code = run_command(step)
            if code != 0:
                return code
        return 0

    if command in COMMANDS:
        return run_command(command)

    print("Usage: python run.py [ingest|stage|load|transform|dq|export|run]")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())