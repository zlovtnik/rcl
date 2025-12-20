# Schema Registry with proper JMX file permissions
FROM confluentinc/cp-schema-registry:7.5.0

USER root

# Copy JMX files
COPY jmxremote.password /opt/schema-registry/jmxremote.password
COPY jmxremote.access /opt/schema-registry/jmxremote.access

# Override entrypoint to fix permissions
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
