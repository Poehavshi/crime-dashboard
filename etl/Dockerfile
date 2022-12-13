FROM apache/airflow:2.4.3
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

user root
# Install openJDK for spark
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean; \