FROM apache/spark:3.5.7

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir --target=/tmp/python-packages -r requirements.txt

ENV PYTHONPATH=/tmp/python-packages

COPY streaming.py /opt/spark/streaming.py

USER spark

CMD ["/opt/spark/bin/spark-submit","--conf", "spark.jars.ivy=/tmp/.ivy2","--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7","/opt/spark/streaming.py"]

