import sqlite3
from sqlite3 import Error

PATH_TO_DB = "data/amazon_product_data.db"


def create_connection() -> sqlite3.Connection:
    connection = None

    try:
        connection = sqlite3.connect(PATH_TO_DB)
    except Error as e:
        print(e)

    return connection
