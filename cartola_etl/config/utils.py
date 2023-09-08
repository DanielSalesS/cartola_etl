from dotenv import load_dotenv, find_dotenv


def load_env():
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
