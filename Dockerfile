FROM node:14

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install

# Instala PM2 globalmente
RUN npm install pm2 -g

COPY . .

EXPOSE 3000

# Comando para iniciar la aplicación con PM2
CMD ["pm2-runtime", "start", "server.js", "--name", "my-node-app"]
