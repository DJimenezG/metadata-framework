\# Metadata-Driven Spark Framework ###############



Este proyecto implementa un motor de procesamiento de datos escalable y configurable usando Apache Spark y Scala, dirigido completamente por metadatos.



\## Objetivo #####################################



Ejecutar pipelines de lectura, transformación y escritura de datos de forma dinámica según un fichero de configuración JSON (`metadata.json`). El motor está diseñado para adaptarse a nuevos casos sin modificar el código fuente, simplemente cambiando los metadatos.



\## Estructura del proyecto



```

.

├── data/                         # Datos de entrada (2024 y 2025)

├── project/                     # Archivos internos de SBT

├── src/

│   ├── main/

│   │   ├── scala/               # Código fuente del framework

│   │   ├── resources/           # metadata.json y logback.xml

│   └── test/                    # Tests unitarios con ScalaTest

├── build.sbt                    # Configuración de dependencias

├── run.sh                       # Script de ejecución

└── README.md                    # Este archivo

```



\## Funcionalidades implementadas ################



\- \*\*Lectura dinámica\*\* desde archivos CSV según metadata

\- \*\*Transformaciones configurables\*\* (`add\_fields`, `filter`)

\- \*\*Escritura\*\* en modo `overwrite`, `append` y `merge` en formato Delta Lake

\- \*\*Comparación de cambios\*\* entre datasets 2024 y 2025

\- \*\*Validación de calidad del dato\*\* (`Ambos sexos != Hombres + Mujeres`)

\- \*\*Logging configurable\*\* con `logback.xml`

\- \*\*Ejecutable como JAR\*\* desde contenedor Docker con Spark



\## Construcción #################################



```bash

sbt assembly

```



\## Docker (Entorno de ejecución) ################



```bash

docker compose up -d --build

docker exec -it spark-standalone bash

./scripts/run\_main.sh 2024

./scripts/run\_main.sh 2025

```



\##  Análisis posterior ##########################



Incluye scripts para inspeccionar los resultados Delta con Spark SQL y validar el correcto funcionamiento del flujo.



\## Arquitectura Cloud sugerida (AWS) ############



\- \*\*S3\*\* como origen y destino de datos

\- \*\*AWS Glue\*\* o \*\*EMR\*\* para ejecutar Spark

\- \*\*Airflow (MWAA)\*\* para orquestación

\- \*\*RDS o DynamoDB\*\* como almacén de metadatos

\- \*\*GitHub Actions\*\* para CI/CD



El diseño completo está en la presentación `Implantación\_Motor\_Metadata\_AWS.pptx`.



\## Autor ########################################



\- \*\*Nombre:\*\* Daniel Jimenez Grande

\- \*\*GitHub:\*\* \[https://github.com/DJimenezG](https://github.com/DJimenezG)



