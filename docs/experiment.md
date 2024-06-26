# Experimento

## 1. Selecionar Métricas e Configurações

As métricas escolhidas foram:
* Consumer
    * bytes-consumed-rate
    * records-consumed-rate
    * records-lag-max
    * request-latency-avg
    * request-latency-max

As configurações escolhidas foram: 

* Consumer
    * max.partition.fetch.bytes
        * 1048576 (Padrão)
        * 943718  (90%)
        * 838860  (80%)
        * 734000  (70%)
        * 629145  (60%)
        * 524288  (50%)
        * 1153433 (110%)
        * 1258291 (120%)
        * 1363148 (130%)
        * 1468006 (140%)
        * 1572864 (150%)
    * fetch.max.bytes
        * 52428800 (Padrão)
        * 47185920 (90%)
        * 41943040 (80%)
        * 36700160 (70%)
        * 31457280 (60%)
        * 26214400 (50%)
        * 57671680 (110%)
        * 62914560 (120%)
        * 68157440 (130%)
        * 73400320 (140%)
        * 78643200 (150%)
    * max.poll.records
        * 500 (Padrão)
        * 450 (90%)
        * 400 (80%)
        * 350 (70%)
        * 300 (60%)
        * 250 (50%)
        * 550 (110%)
        * 600 (120%)
        * 650 (130%)
        * 700 (140%)
        * 750 (150%)
    * receive.buffer.bytes
        * 65536 (Padrão)
        * 58982 (90%)
        * 52428 (80%)
        * 45875 (70%)
        * 39321 (60%)
        * 32768 (50%)
        * 72089 (110%)
        * 78643 (120%)
        * 85196 (130%)
        * 91750 (140%)
        * 98304 (150%)
    * send.buffer.bytes
        * 131072 (Padrão)
        * 117964 (90%)
        * 104857 (80%)
        * 91750  (70%)
        * 78643  (60%)
        * 65536  (50%)
        * 144179 (110%)
        * 157286 (120%)
        * 170393 (130%)
        * 183500 (140%)
        * 196608 (150%)
* Producer
    * batch.size
        * 16384 (Padrão)
        * 14745 (90%)
        * 13107 (80%)
        * 11468 (70%)
        * 9830  (60%)
        * 8192  (50%)
        * 18022 (110%)
        * 19660 (120%)
        * 21299 (130%)
        * 22937 (140%)
        * 24576 (150%)
    * linger.ms
        * 0 (Padrão)
        * 100
        * 200
        * 300
        * 400
        * 500
    * max.request.size
        * 1048576 (Padrão)
        * 943718  (90%)
        * 838860  (80%)
        * 734003  (70%)
        * 629145  (60%)
        * 524288  (50%)
        * 1153433 (110%)
        * 1258291 (120%)
        * 1363148 (130%)
        * 1468006 (140%)
        * 1572864 (150%)
    * receive.buffer.bytes
        * 32768 (Padrão)
        * 29491 (90%)
        * 26214 (80%)
        * 22937 (70%)
        * 19660 (60%)
        * 16384 (50%)
        * 36044 (110%)
        * 39321 (120%)
        * 42598 (130%)
        * 45875 (140%)
        * 49152 (150%)
    * send.buffer.bytes
        * 131072 (Padrão)
        * 117964 (90%)
        * 104857 (80%)
        * 91750  (70%)
        * 78643  (60%)
        * 65536  (50%)
        * 144179 (110%)
        * 157286 (120%)
        * 170393 (130%)
        * 183500 (140%)
        * 196608 (150%)

## Verificando como as métricas se comportam quando há a variação no valor dos parametros de configuração

A cada rodada de execução serão criados um conjunto de configurações de teste e um conjunto de 
configurações padrão, e as métricas serão comparadas.