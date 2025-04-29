# Otimização

Referências:
* https://newrelic.com/blog/how-to-relic/tuning-apache-kafka-consumers#toc-understanding-kafka-consumer-fetch-batching
* https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html
* https://strimzi.io/blog/2021/01/07/consumer-tuning/
* https://www.redpanda.com/guides/kafka-performance-kafka-performance-tuning
* https://medium.com/@alvarobacelar/otimizando-kafka-consumers-ec46342dba3d
* https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/2.5/html/kafka_configuration_tuning/con-consumer-config-properties-str#con-consumer-config-properties-throughput-str
* https://medium.com/@zdb.dashti/explore-kafka-consumer-strategies-to-improve-kafka-performance-a781488d30cb
* https://www.meshiq.com/top-10-kafka-configuration-tweaks-for-performance/
* https://stackoverflow.blog/2024/09/04/best-practices-for-cost-efficient-kafka-clusters/

## Seleção de Parametros

1. Baseado em Regras
2. Adaptativo

## Máquina de Estados

Para caracterizar o estado da performance do Kafka Stream vamos definir 3 variáveis de análise.

1. Vazão
    1. Sustentável
    2. Insustentável
2. Recursos
    1. Subutilizados
    2. Saturados
3. Parâmetros
    1. Subótimos
    2. Ótimos

A otimização é necessária se e somente se a Vazão é Insustentável. Assim ela será definida por 2 passos.

1. Uso ótimo de recursos
2. Busca por parâmetros ótimos

## 1. Uso ótimo de recursos
O Stream deve aumentar o número de threads caso exista CPU e Memória disponível. Baseado em calculo de das métricas CPU Load e memory usage.

## 2. Busca por parâmetros ótimos

* Aumentar `fetch.min.bytes` e `fetch.max.wait.ms` para aumentar vazão 
* Reduzir `max.poll.records` para diminutir latência (Não necessário em caso de Vazão Insustentável)
* Aumentar `max.poll.records` para aumentar vazão


## Outras métricas relevantes

1. Tamanho médio da mensagem
    > Pode impactar `fetch.min.bytes`, `max.partition.fetch.bytes` e `fetch.max.bytes` 

