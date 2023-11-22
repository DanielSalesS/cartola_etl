from pathlib import Path
import subprocess


def execute_script(script_path):
    """
    Executes a Python script using subprocess.

    Runs a Python script using the subprocess module.

    Args:
        script_path (str): Path to the Python script to be executed.

    Returns:
        None
    """
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print("Script executado com sucesso!")
    else:
        print("Erro ao executar o script:")
        print(result.stderr)


def run_scripts():
    """
    Runs a series of scripts.

    Runs multiple Python scripts located in a specific directory using the 
    `execute_script` function.

    Returns:
        None
    """
    scripts = [
        "create_database.py",
        "create_tables.py",
    ]

    script_dir = Path(__file__).parent.joinpath("cartola_etl/scripts")

    for script in scripts:
        script_path = script_dir / script
        execute_script(script_path)


run_scripts()
