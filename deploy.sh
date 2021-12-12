docker stop nlp-processing
docker stop logstash-service-nlp

git pull origin master

docker-compose up --build -d nlp-processing logstash-service
