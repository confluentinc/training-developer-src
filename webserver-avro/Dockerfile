FROM node:dubnium-stretch
RUN apt-get update && apt-get install -y kafkacat && \
    wget -qO - https://packages.confluent.io/deb/5.3/archive.key | apt-key add - && \
    echo "deb [arch=amd64] http://packages.confluent.io/deb/5.3 stable main" >> /etc/apt/sources.list && \
    mkdir -p /home/node/app/node_modules && \
    chown -R node:node /home/node/app && \
    apt-get update && \
    apt-get install -y confluent-librdkafka-plugins
WORKDIR /home/node/app
COPY package*.json ./
USER node
RUN npm install
COPY --chown=node:node . .
EXPOSE 3000
