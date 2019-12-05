# GDELT Big Data Project

## Przygotowanie
Przed przystąpieniem do pracy na środowisku należy wykonać konfigurację według poniższych kroków:
1. Ściagnięcie obrazu Cloudera Quickstart z [oficjalnej strony producenta](https://www.cloudera.com/downloads/quickstart_vms/5-13.html).
2. Instalacja oprogramowania Docker (opcjonalne). 
3. Wypakowanie ściągniętej paczki: `tar xzfv cloudera-quickstart-vm-5.13.0-0-beta-docker.tar.gz`.
4. Lokalne zaimportowanie obrazu: `docker import cloudera-quickstart-vm-5.13.0-0-beta-docker.tar`.
5. Skonfigurowanie pliku Dockerfile, aby wskazywał na `image id` zaimportowanego obrazu.
6. W katalogu z Dockerfile uruchomić: `sudo docker build -f Dockerfile -t cloudera-quickstart .`.

## Uruchamianie obrazu Dockera

```
sudo docker run -it  -p 80:80  -p 4040:4040  -p 8020:8020  -p 8022:8022  -p 8030:8030  -p 8032:8032  -p 8033:8033  -p 8040:8040  -p 8042:8042  -p 8088:8088  -p 8480:8480  -p 8485:8485  -p 8888:8888  -p 9083:9083  -p 10020:10020  -p 10033:10033  -p 18088:18088  -p 19888:19888  -p 25000:25000  -p 25010:25010  -p 25020:25020  -p 50010:50010  -p 50020:50020  -p 50070:50070  -p 50075:50075  -h quickstart.cloudera --privileged=true -v /home/arek/Documents/Mgr/big-data/gdelt-big-data/:/gdelt-big-data  cloudera-quickstart /usr/bin/docker-quickstart
```

## Przydatne linki
Przydatne linki do stron związanych z ekosystemem Hadoop 2.6.0 dla Cloudery 5.13:
* [Dokumentacja HDFS dla Hadoopa 2.6.0](https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-common/FileSystemShell.html)
