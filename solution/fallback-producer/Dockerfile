FROM node:12-stretch
WORKDIR /app
COPY db db
WORKDIR /app/producer
COPY producer/package.json ./
RUN npm install
COPY producer .
CMD npm start
