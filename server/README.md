# Prerequisites

Before starting the server, ensure you have the following installed:

* Node.js 22
* MongoDB (Ensure `mongod` is available in your system path)
* npm

# Starting the MongoDB Server

```shell
# if not already in the <project-root>/server directory
cd server

mkdir data
mongod --replSet rs0  --dbpath data
```

# Start the Fastify + Socket.io server
```shell
npm run start
```