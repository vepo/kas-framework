FROM apache/activemq-classic:6.1.0
RUN echo ""                                                >> /opt/apache-activemq/conf/log4j2.properties
RUN echo "log4j2.logger.activemq.name=org.apache.activemq" >> /opt/apache-activemq/conf/log4j2.properties
RUN echo "log4j2.logger.activemq.level=INFO"               >> /opt/apache-activemq/conf/log4j2.properties

RUN echo ""                                                                                            >> /opt/apache-activemq/conf/log4j2.properties
RUN echo "log4j2.logger.activemq.name=org.apache.activemq.artemis.core.protocol.stomp.StompConnection" >> /opt/apache-activemq/conf/log4j2.properties
RUN echo "log4j2.logger.activemq.level=DEBUG"                                                          >> /opt/apache-activemq/conf/log4j2.properties

RUN echo "" >> /opt/apache-activemq/conf/logging.properties
RUN echo "org.apache.activemq.artemis.core.protocol.stomp.StompConnection = ALL" >> /opt/apache-activemq/conf/logging.properties
