# PostgreSQL Data Ingestion

Este programa ha sido desarrollado para abordar el problema de la inserción eficiente de grandes volúmenes de datos en una base de datos PostgreSQL a partir de archivos SQL. Es especialmente útil para situaciones en las que se enfrentan limitaciones de hardware y se necesita insertar una gran cantidad de datos en poco tiempo.

## Descripción del Proyecto

En el [Curso de Fundamentos de ETL con Python y Pentaho](https://platzi.com/cursos/fundamentos-etl/) de Platzi, se requiere insertar más de 6 millones de registros en una base de datos PostgreSQL que se encuentra levantada con Docker. Muchos estudiantes enfrentan problemas de rendimiento debido a las limitaciones de hardware de sus computadoras y eso hace que les sea muy difícil avanzar con el curso. Este programa ofrece una solución al problema, permitiendo la inserción rápida de datos en la base de datos en menos de 2 minutos (dependiendo de las especificaciones de tu computadora).

## Estructura del Proyecto

El proyecto sigue la siguiente estructura de archivos y carpetas:

- `app/`

  - `constants.py`: Archivo que contiene una constante que indica cuántos registros se guardarán en cada archivo que se dividirá para el procesamiento. (Puedes cambiarlo según las capacidades de tu computadora)
  - `.env`: Archivo donde se deben colocar las credenciales de la base de datos PostgreSQL.
  - `main.py`: Archivo principal del programa.

- `src/`

  - `archivo.sql`: Archivo SQL que contiene los 6 millones de registros que serán procesados e insertados en la base de datos.

- `requirements.txt`: Archivo que contiene las dependencias del proyecto.

## Configuración

1. Clona este repositorio en tu máquina local.
2. Coloca el archivo `archivo.sql` en la carpeta `src/`.
3. Abre el archivo `.env` y proporciona las credenciales necesarias para acceder a tu base de datos PostgreSQL.

## Uso

1. Ejecuta el programa ejecutando el archivo `main.py`.
2. El programa dividirá el archivo `archivo.sql` en partes más pequeñas y las insertará en la base de datos PostgreSQL.

## Dependencias

Este proyecto requiere las siguientes dependencias, que puedes instalar utilizando pip:

```bash
pip install -r requirements.txt
```
