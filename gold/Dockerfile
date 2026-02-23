FROM spark:3.5.0-python3

WORKDIR /app

# garante que spark-submit esteja no PATH
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH=/app/src

COPY src/ ./src/

# recomendo rodar em modo local dentro do container
CMD ["spark-submit", "--master", "local[*]", "src/main.py"]