# Fix the JAVA_HOME bug in dajobe/hbase
# The image expects Java at /usr/lib/jvm/java-8-openjdk-amd64
# but the actual JDK may be elsewhere — so we install it fresh.
FROM eclipse-temurin:8-jdk-focal

USER root

# Install OpenJDK 8 at the exact path HBase expects
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends openjdk-8-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Ensure JAVA_HOME is set correctly for all HBase scripts
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH
