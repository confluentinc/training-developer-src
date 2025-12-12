FROM node:22-alpine

# Install build dependencies for node-rdkafka native compilation
RUN apk add --no-cache \
    kcat \
    curl \
    python3 \
    make \
    g++ \
    bash \
    librdkafka-dev

# Create app directory and set ownership
RUN mkdir -p /home/node/app && chown -R node:node /home/node/app

WORKDIR /home/node/app

# Copy package files as node user
COPY --chown=node:node package*.json ./

USER node

RUN npm install

COPY --chown=node:node . .

EXPOSE 3000

CMD ["npm", "start"]

