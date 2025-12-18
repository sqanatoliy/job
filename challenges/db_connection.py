from decouple import config
import psycopg2


def get_db_connection():
    """
    Встановлення з'єднання з базою даних PostgreSQL
    """
    return psycopg2.connect(
        dbname=config("DB_NAME"),
        user=config("DB_USER"),
        password=config("DB_PASSWORD"),
        host=config("DB_HOST"),
        port=config("DB_PORT"),
    )