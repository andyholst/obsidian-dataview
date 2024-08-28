FROM node:16-alpine

RUN apk add --no-cache \
    git \
    openssh-client \
    curl

RUN npm install -g npm@9.9.3

COPY docker-files/npm/setup.sh /tmp/setup.sh

RUN sed -i 's/\r$//' /tmp/setup.sh && chmod +x /tmp/setup.sh

WORKDIR /app

ENTRYPOINT ["/tmp/setup.sh"]

CMD ["npm", "run", "build"]
