from pathlib import Path
import subprocess


def execute_script(script_path):
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print("Script executado com sucesso!")
    else:
        print("Erro ao executar o script:")
        print(result.stderr)


def run_scripts():
    scripts = [
        "create_database.py",
        "create_tables.py",
    ]

    script_dir = Path(__file__).parent.joinpath("cartola_etl/scripts")

    for script in scripts:
        script_path = script_dir / script
        execute_script(script_path)


run_scripts()
