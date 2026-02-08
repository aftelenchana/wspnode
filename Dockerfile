FROM node:22-bullseye

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm ci --omit=dev

RUN npm install -g pm2

COPY . .

EXPOSE 3000

CMD ["pm2-runtime", "server.js"]
