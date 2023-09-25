from setuptools import setup, find_packages

setup(
    name="cartola_etl",
    version="1.0.0",
    description="ETL para coleta e processamento de dados da API do Cartola",
    author="Daniel Sales",
    packages=find_packages(),
    install_requires=[
        "python-dotenv==1.0.0",
        "requests==2.31.0",
        "mysql-connector-python==8.1.0",
        "pandas==2.0.3",
        "numpy==1.24.4",
    ],
)
