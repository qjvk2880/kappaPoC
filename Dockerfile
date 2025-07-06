FROM bitnami/spark:3.5

USER root

USER root
RUN echo "appuser:x:1001:1001:appuser:/home/appuser:/bin/bash" >> /etc/passwd
RUN mkdir -p /opt/bitnami/spark/.ivy2 && chmod -R 777 /opt/bitnami/spark/.ivy2

USER 1001