import mysql.connector


class ConnectionManager:
    """
    Manages the connection to the MySQL database.

    A class that manages the connection to the MySQL database using the provided
    credentials.

    Attributes:
        host (str): The host name or IP address of the database server.
        username (str): The username for authentication.
        password (str): The password for authentication.
        database (str): The name of the database.
        port (int): The port number to connect to.
        connection (mysql.connector.connection.MySQLConnection): The connection object
        representing the connection to the database.
    """
    def __init__(self, host, username, password, database, port):
        """
        Initializes a new instance of the class.

        Args:
            host (str): The host name or IP address of the database server.
            username (str): The username for authentication.
            password (str): The password for authentication.
            database (str): The name of the database.
            port (int): The port number to connect to.

        Returns:
            None
        """
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """
        Connects to the MySQL database using the provided credentials.

        Returns:
            mysql.connector.connection.MySQLConnection: The connection object 
            representing the connection to the database.
        """
        if self.connection is None:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.database,
                port=self.port,
            )
        return self.connection

    def close_connection(self):
        """
        Closes the connection to the database.

        This function checks if there is an existing connection to the database.
        If a connection exists, it is closed using the `close()` method.
        After closing the connection, the `connection` attribute is set to `None`.
        
        Returns:
            None
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None
