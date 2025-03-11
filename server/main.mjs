import {MongoClient} from "mongodb";
import {Server} from "socket.io";
import Fastify from "fastify";
import FastifyCors from "@fastify/cors";

const app = Fastify({
    logger: {
        transport: {
            target: "pino-pretty",
        },
    },
});
await app.register(FastifyCors, {
    origin: "*",
});

const io = new Server(app.server, {
    cors: {
        origin: "*",
    },
});

try {
    const mongoClient = new MongoClient("mongodb://localhost:27017",
        {replicaSet: "rs0", directConnection: true});
    const response = await mongoClient.db("admin").admin().command({replSetInitiate: {}});
    console.log(response);
} catch (error) {
    console.error(error);
}

const client = new MongoClient("mongodb://localhost:27017");
const collection = client.db("test").collection("result-cache-0");
const collection2 = client.db("test").collection("result-cache-1");

setInterval(() => {
    const doc = { timestamp: Date.now() };
    collection.insertOne(doc).then(() => {
        app.log.info("Inserted:", doc);
    }).catch(console.error);
}, 20000);

setInterval(() => {
    const doc = { timestamp: Date.now() };
    collection2.insertOne(doc).then(() => {
        app.log.info("Inserted2:", doc);
    }).catch(console.error);
}, 20000);

class MongoConnectionMgr {
    constructor(client, dbName) {
        this.client = client;
        this.dbName =  dbName;
        this.db = client.db(dbName);
        this.collections = new Map;
        //connectionID -> {collectionName: watcher}
        this.watches = new Map();
        //connectionID -> {collectionName: changeBackLog}
        this.changeBuffers = new Map();
        //connectionID -> {collectionName: limit}
        this.changeLimits = new Map();
        //connectionID -> {collectionName: timer}
        this.changeTimers = new Map();
        //connectionID -> {collectionName: emitDelay}
        this.changeEmitDelays = new Map();
        this.init();
    }

    init() {
        io.on("connection", (socket) => {
            socket.on("subscribe", async ({collectionName, pipeline}) => {
                this.subscribe(collectionName, pipeline, socket);
            });

            socket.on("bufferedSubscribe", async ({collectionName, changeLimit, emitDelay, pipeline}) => {
                this.bufferedSubscribe(collectionName, changeLimit, emitDelay, pipeline, socket);
            });

            socket.on("find", async ({requestId, collectionName, query, options}) => {
                app.log.info(requestId)
                app.log.info(query);
                app.log.info(options);
                try{
                    const findResults = await this.executeFind(collectionName, query, options);
                    app.log.info(findResults);
                    socket.emit("findResults", { requestId: requestId, results: findResults });
                } catch (error){
                    app.log.error(`Error finding documents in ${collectionName}: ${error.message}`);
                    socket.emit("findError", { requestId, message: "Failed to find documents." });
                }
            });

            socket.on("disconnect", (reason) => {
                app.log.info(`Client disconnected: ${socket.id}`);
                this.cleanup(socket.id);
            });
        });
    }

    //TODO: Add checks to both subscribe and buffered subscribe to check for duplicate subscribes
    async subscribe(collectionName, pipeline, socket) {
        let connectionID = socket.id;
        if (false === this.collections.has(collectionName)) {
            const databaseCollections = await this.db.listCollections().toArray();
            let collectionExists = databaseCollections.some(collection => collection.name === collectionName);
            if (false === collectionExists) {
                socket.emit("subscribeError", { collectionName, message: `Collection name ${collectionName} does not exist in ${this.dbName}` });            
            } else {
                this.collections.set(collectionName, this.client.db(this.dbName).collection(collectionName));
            }
        }  
        let requestedCollection = this.collections.get(collectionName);

        socket.emit("initialDocuments", {
            collectionName: collectionName,
            data: await requestedCollection.aggregate(pipeline).toArray(),
        });

        if (false === this.watches.has(connectionID)) {
            this.watches.set(connectionID, {});
            app.log.info(`Watching first Collection: ${collectionName}`);
        }

        //need to test that additional collections are handled correctly once the client can use the same socket for all connections
        this.watches.get(connectionID)[collectionName] = requestedCollection.watch(pipeline);
        app.log.info(`Watching Collection: ${collectionName}`);
        
        this.watches.get(connectionID)[collectionName].on("change", (change) => {
            socket.emit("updateDocuments", {
                collectionName: collectionName,
                change: change,
            });
        });
    }

    async bufferedSubscribe(collectionName, changeLimit, emitDelay, pipeline, socket) {
        let connectionID = socket.id;
        app.log.info(`Collection name ${collectionName} requested`);
        app.log.info(`ConnectionID ${connectionID}`);
        if (false === this.collections.has(collectionName)) {
            const databaseCollections = await this.db.listCollections().toArray();
            let collectionExists = databaseCollections.some(collection => collection.name === collectionName);
            if (false === collectionExists) {
                socket.emit("subscribeError", { collectionName, message: `Collection name ${collectionName} does not exist in ${this.dbName}` });       
            } else {
                this.collections.set(collectionName, this.client.db(this.dbName).collection(collectionName));
            }
        }  
        let requestedCollection = this.collections.get(collectionName);

        socket.emit("initialDocuments", {
            collectionName: collectionName,
            data: await requestedCollection.aggregate(pipeline).toArray(),
        });

        if (false === this.watches.has(connectionID)) {
            this.watches.set(connectionID, {});
            this.changeBuffers.set(connectionID, {});
            this.changeLimits.set(connectionID, {});
            this.changeEmitDelays.set(connectionID, {});
            this.changeTimers.set(connectionID, {});
            app.log.info(`Buffered Watching first Collection: ${collectionName}`);
        }
        //needs to be tested once the client can use the same socket for all connections
        //check if already subscribed to collection
        this.watches.get(connectionID)[collectionName] = requestedCollection.watch(pipeline);
        this.changeBuffers.get(connectionID)[collectionName] = [];
        this.changeLimits.get(connectionID)[collectionName] = changeLimit;
        this.changeEmitDelays.get(connectionID)[collectionName] = emitDelay;
        this.changeTimers.get(connectionID)[collectionName] = null;
        app.log.info(`Buffered Watching Collection: ${collectionName}`);
    
        this.watches.get(connectionID)[collectionName].on("change", (change) => {
            // Buffer the change for bulk emission
            this.bufferChangeForEmission(connectionID, collectionName, change, socket);
        });
    }

    bufferChangeForEmission(connectionID, collectionName, change, socket) {
        const buffer = this.changeBuffers.get(connectionID)[collectionName];
        buffer.push(change);

        // Clear any existing timer
        if (this.changeTimers.get(connectionID)[collectionName]) {
            clearTimeout(this.changeTimers.get(connectionID)[collectionName]);
        }

        // Set a new timer to emit the changes
        this.changeTimers.get(connectionID)[collectionName] = setTimeout(() => {
            this.emitBufferedChanges(connectionID, collectionName, socket);
        }, this.changeEmitDelays.get(connectionID)[collectionName]);

        const bufferLimit = this.changeLimits.get(connectionID)[collectionName];
        const bufferLength = this.changeBuffers.get(connectionID)[collectionName].length;
        if(bufferLength >= bufferLimit){
            this.emitBufferedChanges(connectionID, collectionName, socket);
        }
    }

    emitBufferedChanges(connectionID, collectionName, socket) {
        const buffer = this.changeBuffers.get(connectionID)[collectionName];
        if (buffer.length > 0) {
            socket.emit("bufferedUpdateDocuments", {
                collectionName: collectionName,
                changes: buffer,
            });
            buffer.length = 0; // Clear the buffer
        }
    }

    cleanup(connectionID) {
        if (this.watches.has(connectionID)) {
            for (let key in this.watches.get(connectionID)) {
                const watcher = this.watches.get(connectionID)[key];
                watcher.close(); 
                delete this.watches.get(connectionID)[connectionID];
                app.log.info(`Cleaned up resources for connection ID: ${connectionID}`);
            }
            this.watches.delete(connectionID);
            app.log.info(`Removed ${connectionID} from watches map`);
        }
    }

    async executeFind(collectionName, query, options) {
        if (false === this.collections.has(collectionName)) {
            const databaseCollections = await this.db.listCollections().toArray();
            let collectionExists = databaseCollections.some(collection => collection.name === collectionName);
            if (false === collectionExists) {
                throw new Error(`Collection name ${collectionName} does not exist in ${this.dbName}`);            
            } else {
                this.collections.set(collectionName, this.client.db(this.dbName).collection(collectionName));
            }
        }    
        const collection = this.collections.get(collectionName);
        app.log.info(query);
        app.log.info(options);
        return await collection.find(query, options).toArray();
    }

}

let mCollect = new MongoConnectionMgr(client, "test");

await app.listen({port: 5000});