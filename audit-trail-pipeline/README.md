# EVENT AUDIT LOG PIPELINE

### REQUIREMENT
- Admin need to analyze daily user analytics by product including the new products
- It help the buniness to take timely decisions to boost the service or add campain more for the most visited or purchased products

### PRE-REQUISITES
- docker


### PIPELINE COMPONENTS
- data-generator
- kafka-connect
- spark-analytics-job

## HOW TO SETUP
- Execute below command in /kafka-connect directory to make up all required containers for this pipeline
- docker-compose -f docker-compose.yml up -d

![image](https://user-images.githubusercontent.com/28490692/198989693-396a1953-acb0-4c8e-8415-b5c42108086f.png)


## DATA GENERATOR
It`s a module developed in spring-kotlin using mongo db. 
It`s sole purpose is to generate dummy data of user interactions and save it in mongo db
Later on , we shall use this data in our pipeline to generate insights.

- Use below url to generate test data
- http://localhost:8080/generateEvents
![image](https://user-images.githubusercontent.com/28490692/198989935-8d098b57-8374-48f2-8427-f0fc29e9b220.png)


## KAFKA CONNECT
- It`s a distributed configuration base engine that is used to connect kafka with number of external systems.
- Once the docker compose is up, it also contains a container for kafka-connect
- You can interact with kafka connect using its rest api`s
- #GET http://localhost:8083/connectors      ::return the list of all available connectors
- #POST http://localhost:8083/connectors     :: takes json body and create a connector 
- HERE is mongo json body for above api

        {
          "name" : "mongo-source",
          "config":{
                      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
                      "connection.uri": "mongodb://mongo1:27017",
                      "database": "eventlog",
                      "collection": "auditlog",
                      "topic.prefix": "mongo-01-",
                      "tasks.max":1,
                      "publish.full.document.only":"true",
                      "key.converter":"org.apache.kafka.connect.storage.StringConverter",
                      "value.converter":"org.apache.kafka.connect.json.JsonConverter",
                      "pipeline":"[{'$match': { '$and': [{'operationType': 'insert'}] } } ]"  

          }
      }

![image](https://user-images.githubusercontent.com/28490692/198991230-f16f99d8-d703-48bd-bcf1-7d7d3eda9bea.png)
#### DELETE EXISTING CONNECTOR
![image](https://user-images.githubusercontent.com/28490692/198991391-42850019-2ce1-40b1-9db6-58bec0ff7e8d.png)

#### IMPORTANT NOTE
- Below command auto download required driver to connect to mongo.
- #### confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.8.0
- To link manual jar with docker connect use below command:
- #### docker cp kafka-connect:/usr/share/confluent-hub-components ./data/connect-jars

## SPARK ANALYTIC JOB
This module is based on scala.
It manages below operations:
- Reads data from kafka
- Converts to json from binary
- Aggregate data using spark sql
- Publish summary data to kafka topic
- Dump summary to maria db for future reference

![image](https://user-images.githubusercontent.com/28490692/198993973-4c9f47f0-42bd-4718-810e-9fa67b14ca1a.png)



### NOTES


#### connect to docker container
docker  exec -it kafka-connect bash
docker  exec -u 0 -it kafka-connect bash

#### view file inside docker container
cat /etc/kafka/connect-log4j.properties view file

#### Edit files inside docker container
yum install nano
nano /etc/kafka/connect-log4j.properties


#### KAFKA DROP
- You can connect to kafka drop UI using http://localhost:9000/

![image](https://user-images.githubusercontent.com/28490692/198994923-87ce18fa-ef7a-4334-b6a8-a83936bfad27.png)



 
