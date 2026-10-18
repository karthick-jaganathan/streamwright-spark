FROM apache/hive:4.0.0
USER root
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/ && \
    cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/
COPY docker/hms/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
USER hive
