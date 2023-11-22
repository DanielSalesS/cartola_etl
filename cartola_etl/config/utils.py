from dotenv import load_dotenv, find_dotenv


def load_env():
    """
    Load environment variables from a .env file.

    This function finds the path to the .env file using the `find_dotenv` function and
    loads the variables into the environment using the `load_dotenv` function.
    
    Returns:
        None
    """
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
