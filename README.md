# Practicando RDD en Databricks

![RDD en Databricks](https://github.com/user-attachments/assets/0f9d7e23-bf1e-45bc-b2b5-f94bad6001bd)

Este repositorio contiene ejercicios prácticos sobre **RDDs (Resilient Distributed Datasets)** en **Apache Spark**, enfocados en **transformaciones** y **acciones**. Los RDDs son una de las estructuras de datos clave de Spark que permiten realizar operaciones distribuidas sobre grandes volúmenes de datos de manera eficiente.

## Objetivo

El objetivo de este repositorio es proporcionar ejemplos prácticos de cómo crear y manipular **RDDs** en Spark, con un enfoque en las transformaciones y acciones que podemos realizar sobre ellos. Este ejercicio está diseñado para aprender a usar Spark en un entorno distribuido y aplicar transformaciones y acciones de manera eficiente.

### Qué encontrarás en este repositorio

- **Notebooks en Databricks**: Ejercicios prácticos realizados en Databricks, donde se muestra cómo trabajar con **RDDs** utilizando transformaciones como `map`, `filter`, `flatMap`, y acciones como `collect`, `count`, `reduce`.
- **Datasets**: Archivos de datos que se usan para las transformaciones y análisis de los RDDs. Puedes encontrarlos en la carpeta `datasets/`.
- **Imágenes**: Visualizaciones generadas durante los análisis, como gráficos de tendencias o histogramas, que ilustran los resultados de las transformaciones.

## Estructura del Repositorio

- **notebook**: Contenido principal de la páctica.
- **datasets/**: Archivos de datos (CSV, Parquet, etc.) utilizados para realizar las transformaciones y análisis en los RDDs.
- **images/**: Imágenes generadas a partir de las visualizaciones y resultados de las transformaciones y análisis.
- **solutions/**: Contiene el notebook solucionado con algunos recursos extra que se piden en la práctica.
- **README.md**: Este archivo, que proporciona información sobre el proyecto y cómo empezar.

## Requisitos Previos

Para poder ejecutar los notebooks en **Databricks**, necesitarás:

- **Cuenta en Databricks**: Si aún no tienes una cuenta, puedes crear una [aquí](https://databricks.com/).
- **Acceso a los archivos de datos**: Asegúrate de cargar los datasets en Databricks en la carpeta de **Inputs** antes de ejecutar el notebook.

>Nota: Se recomienda usar `Databricks` para esta práctica, ya que las rutas están adaptadas a su entorno. Sin embargo, también es posible realizarla en otros notebooks con ajustes en las rutas.

## Cómo ejecutar el notebook

1. **Clona el repositorio**:
    ```bash
    git clone https://github.com/leo-narvaez/rdd-transformations-spark.git
    cd rdd-transformations-spark
    ```

2. **Sube el dataset a Databricks**:
    - Entra a **Databricks** y crea un nuevo **cluster** si aún no tienes uno.
    - Carga los archivos clonados al **DBFS** de **Databricks**

3. **Ejecuta el notebook**:
    - Abre los notebooks dentro de la plataforma **Databricks**.
    - Asegúrate de que tu cluster esté en ejecución y comienza a ejecutar cada celda.
    - En el notebook se encuentran ejemplos de cómo crear RDDs y aplicar transformaciones y acciones.

## Resultados Esperados

Al ejecutar los notebooks, podrás obtener:

- **Transformaciones y acciones realizadas sobre los RDDs**: Verás cómo manipular grandes volúmenes de datos utilizando RDDs en un entorno distribuido.
- **Visualizaciones**: Imágenes que muestran las visualizaciones generadas a partir de los datos transformados.
- **Análisis de rendimiento**: En algunos casos, se analizará cómo las diferentes transformaciones afectan el tiempo de ejecución y el rendimiento.
