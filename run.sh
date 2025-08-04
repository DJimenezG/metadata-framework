#!/bin/bash

# 1. Compilar el jar con sbt assembly
echo "🛠️ Compilando proyecto con sbt assembly..."
sbt clean compile assembly

# 2. Copiar jar a carpeta Docker
echo "📦 Copiando JAR al entorno Docker..."
cp target/scala-2.12/metadata-framework-assembly-0.1.jar ../spark-docker/jars/

# 3. Ejecutar en Docker
echo "🚀 Ejecutando spark-submit dentro del contenedor Docker..."
docker exec -it spark-standalone spark-submit \
  --class main.Main \
  --master local[*] \
  /app/jars/metadata-framework-assembly-0.1.jar \
  /app/metadata/metadata.json
