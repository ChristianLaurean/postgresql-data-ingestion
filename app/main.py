import time
import psycopg2
import logging
import os
import shutil
from constants import RECORD_PER_FILE
from typing import Generator
from tqdm import tqdm
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(asctime)s - Message: %(message)s", datefmt="%Y-%m-%d")




class DatabaseManager:
    """Clase para gestionar la conexión y manipulación de una base de datos PostgreSQL."""

    def __init__(self, dbname, user, password, host, port):
        """
        Inicializa la conexión a la base de datos.

        Args:
            dbname (str): Nombre de la base de datos.
            user (str): Nombre de usuario para acceder a la base de datos.
            password (str): Contraseña del usuario.
            host (str): Dirección del host donde se encuentra la base de datos.
            port (int): Número de puerto en el que está escuchando la base de datos.
        """
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        logging.info(f"Connected to PostgreSQL in {user}")




    def create_table(self, create_table_query: Generator):
        """
        Crea una tabla en la base de datos.

        Args:
            create_table_query (generator): Consulta SQL para crear la tabla.
        """
        cursor = self.connection.cursor()
        logging.info("Creating Table...")
        try:
            cursor.execute(f"{list(create_table_query)[0][0]};")
            logging.info("Created table!!!")
            self.connection.commit()
        except psycopg2.Error as err:
            self.connection.rollback()
            logging.error(f"Error creating table: {err}")




    def insert_data(self):
        """
        Inserta datos en la tabla de la base de datos.
        """
        cursor = self.connection.cursor()
        try:
            sql_files_path = os.path.join(os.getcwd(), "batched_sql")
            sql_files = os.listdir(sql_files_path)

            for sql_file in tqdm(sql_files, desc="Inserting records"):
                with open(os.path.join(sql_files_path, sql_file)) as f:
                    sql_query = f.read()
                    cursor.execute(sql_query)
                self.connection.commit()
            logging.info("Inserted data")
        except psycopg2.Error as err:
            self.connection.rollback()
            logging.error(f"Error inserting data: {err}")
        finally:
            cursor.close()




class SQLFileLoader:
    """Clase para cargar consultas SQL desde archivos."""

    @staticmethod
    def find_sql_file() -> str:
        """
        Busca y devuelve la ruta del archivo SQL en la carpeta "src".

        Returns:
            str: Ruta del archivo SQL encontrado.
        Raises:
            FileNotFoundError: Si no se encuentra ningún archivo SQL.
        """
        try:
            logging.info("seeking out file_sql")
            path_file = os.path.join(os.getcwd(), "src")
            for file in os.listdir(path_file):
                if file.endswith(".sql"):
                    return os.path.join(path_file, file)
            raise FileNotFoundError
        except FileNotFoundError:
            logging.error("No SQL file found in src folder")




    @staticmethod
    def create_folder():
        """
        Crea una carpeta para almacenar archivos SQL divididos.
        """
        file_path = os.path.join(os.getcwd(), "batched_sql")
        os.makedirs(file_path, exist_ok=True)
        return file_path




    @classmethod
    def extract_queries(cls) -> Generator:
        """
        Extrae las consultas SQL del archivo encontrado.

        Yields:
            str: Consulta SQL del archivo.
        """
        path_file = cls.find_sql_file()
        with open(path_file, mode="r") as file:
            yield file.read().split(";")




    @classmethod
    def divide_files(cls):
        """
        Divide el archivo SQL en archivos más pequeños.
        """
        query_sql = cls.extract_queries()
        query_sql = list(query_sql)[0][1]
        values = query_sql.split("values")
        data_values = values[1].split("\n")
        query_insert = values[0] + " " + "values".strip()
        current_records = 0
        file_content = []
        path = cls.create_folder()
        page = 1

        for data_value in data_values:
            # Agregar la consulta SQL de inserción al archivo nuevo
            if current_records == 0:
                file_content.append(query_insert)

            current_records += 1

            # Verificar si se alcanzó el límite de registros por archivo
            if current_records == RECORD_PER_FILE:
                record_sql = data_value.strip().rstrip(",") + ";" + "\n"
                file_content.append(record_sql)
                # Escribir el archivo y reiniciar los contadores
                cls.write_sql_file(file_content, page, path)
                current_records = 0
                file_content = []
                page += 1
                continue

            # Agregar los valores de los registros al archivo nuevo
            record_sql = data_value.strip() + "\n"
            file_content.append(record_sql)

        # Escribir el archivo si hay contenido restante
        if file_content:
            cls.write_sql_file(file_content, page, path)




    @staticmethod
    def write_sql_file(content: list, page: int, path: str):
        """
        Escribe el contenido en un archivo SQL.

        Args:
            content (list): Contenido del archivo SQL.
            page (int): Número de página para el nombre del archivo.
            path (str): Ruta de la carpeta donde se guardará el archivo.
        """
        file_path = os.path.join(path, f"part_{page}.sql")
        with open(file_path, mode="w") as file:
            file.writelines(content)




    @classmethod
    def remove_folder(cls):
        """
        Elimina la carpeta y todos los archivos SQL que contiene.
        """
        file_path = os.path.join(os.getcwd(), "batched_sql")
        shutil.rmtree(file_path, ignore_errors=True)




def main():
    """Función principal del programa."""
    load_dotenv()

    db_manager = DatabaseManager(
        os.getenv("dbname"),
        os.getenv("user"),
        os.getenv("password"),
        os.getenv("host"),
        os.getenv("port")
    )

    queries = SQLFileLoader.extract_queries()
    db_manager.create_table(queries)
    SQLFileLoader.divide_files()
    db_manager.insert_data()
    SQLFileLoader.remove_folder()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    logging.info(f"Execution time: {execution_time} seconds")
