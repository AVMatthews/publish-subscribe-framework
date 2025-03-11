import {useEffect, useRef, useState} from "react";
import {io} from "socket.io-client";

const ResponseType = {
    INTIAL: "initialDocuments",
    UPDATE: "updateDocuments",
    BULK_UPDATE: "bulkUpdateDocuments"
}

//could make this take a port for connecting to different places
//Can only subsription to a collection once per socket ... could add another ID
class MongoConnection {
    constructor() {
        this.socket = io("http://localhost:5000");
        this.requestCallbacks = {}; 
        //one handler per subscription
        // -> Could modify to accept handlers for different kinds of packets
        //      (ie. initalDocuments, updateDocument, bufferedUpdateDocuments, Error, etc.)
        this.subscriptionHandlers = {};
        this.init();
    }

    init() {
        //Modify to take multiple onUpdates which are called based on collection?
        this.socket.on("initialDocuments", ({collectionName, data}) => {
            this.subscriptionHandlers[collectionName]({
                type: ResponseType.INTIAL,
                results: data,
            });
        });

        this.socket.on("updateDocuments", ({collectionName, change}) => {
            this.subscriptionHandlers[collectionName]({
                type: ResponseType.UPDATE,
                results: change,
            });
        });

        this.socket.on("bufferedUpdateDocuments", ({collectionName, changes}) => {
            this.subscriptionHandlers[collectionName]({
                type: ResponseType.BULK_UPDATE,
                results: changes,
            });
        });

        this.socket.on("findResults", ({ requestId, results }) => {
            //console.log("request ID:" + requestId);
            console.log(results);
            const callback = this.requestCallbacks[requestId];
            if (callback) {
                callback(results);
                delete this.requestCallbacks[requestId]; // Clean up
            }
        });

        this.socket.on("findError", ({ requestId, message }) => {
            console.log(requestId);
            console.log(message);
        });

        this.socket.on("subscribeError", ({ collectionName, message }) => {
            console.log(collectionName);
            console.log(message);
        });
    }

    subscribe(requestedCollection, pipeline, handler){
        this.subscriptionHandlers[requestedCollection] = handler;
        this.socket.emit("subscribe", {
            collectionName: requestedCollection,
            pipeline: pipeline,
        });
    }

    //buffered subscribe
    //server buffers packets until either change limit or emit delay is met
    //change limit = number of changes
    //emit delay = number of milliseconds until emit
    bufferedSubscribe(requestedCollection, changeLimit, emitDelay, pipeline, handler){
        this.subscriptionHandlers[requestedCollection] = handler;
        this.socket.emit("bufferedSubscribe", {
            collectionName: requestedCollection,
            changeLimit: changeLimit,
            emitDelay: emitDelay,
            pipeline: pipeline,
        });
    }

    find(collectionName, query, findOptions, callback){
        const requestId = Date.now(); // Generate a unique request ID
        //console.log(requestId);
        this.requestCallbacks[requestId] = callback; // Store the callback
        this.socket.emit("find", {
            requestId: requestId,
            collectionName: collectionName,
            query: query,
            options: findOptions,
        });
    }

    //disconnect
    endConnection(){
        this.socket.disconnect("manual close");
    }

}

const App = () => {
    const [documents, setDocuments] = useState([]);
    const hasInitedRef = useRef(false);

    useEffect(() => {
        if (true === hasInitedRef.current) {
            return;
        }
        hasInitedRef.current = true;

        const collectionName = "result-cache-0";
        const collectionName2 = "result-cache-1";

        const doUpdate = (change) => {
            if (change && change.operationType) {
                setDocuments((prevDocs) => {
                    if (change.operationType === "insert") {
                        return [...prevDocs, change.fullDocument];
                    } else if (change.operationType === "delete") {
                        return prevDocs.filter(doc => doc._id !== change.documentKey._id);
                    } else if (change.operationType === "update") {
                        return prevDocs.map(doc => doc._id === change.documentKey._id
                            ? { ...doc, ...change.updateDescription.updatedFields }
                            : doc);
                    } else {
                        console.log("Unknown operation type:", change.operationType);
                    }
                    return prevDocs;
                });
            }  else {
                console.error("Unexpected change structure:", change);
            } 
        };

        const handleUpdate = ({ type, results }) => {
            const change = results;
            //console.log(type);
            //console.log(results);
            if (type === ResponseType.INTIAL) {
                setDocuments(change);
            } else if (type === ResponseType.UPDATE) {
                doUpdate(change);
            } else if (type === ResponseType.BULK_UPDATE) {
                for (const c of change) {
                    //console.log(c);
                    doUpdate(c);
                }
            }
            //Add else error case   
        };

        const handleUpdate2 = ({ type, results }) => {
            console.log("Handling updates from second collection");
        }

        let mCollect = new MongoConnection();
        const startOfDay = new Date();
        startOfDay.setHours(0, 0, 0, 0); // Start of today
        const startOfDayMillis = startOfDay.getTime(); // Get milliseconds

        const endOfDay = new Date();
        endOfDay.setHours(23, 59, 59, 999); // End of today
        const endOfDayMillis = endOfDay.getTime(); // Get milliseconds

        console.log("Start of Day (ms):", startOfDayMillis);
        console.log("End of Day (ms):", endOfDayMillis);

        //example pipeline
        /* const pipeline = [
            {
                $match: {
                    timestamp: {
                        $gte: startOfDayMillis,
                        $lte: endOfDayMillis
                    }
                }
            }
        ]; */
        const pipeline = [];
        
        //example subscribe
        mCollect.subscribe(collectionName, pipeline, handleUpdate);
        console.log("subscribed")

        //mCollect.subscribe(collectionName2, pipeline, handleUpdate2);
        //console.log("subscribed to second collection")

        //example buffered subscribe
        //mCollect.bufferedSubscribe(collectionName, 2, 40000, pipeline, handleUpdate);
        //console.log("buffered subscribe")

        const handleResults = ({ results }) => {
            console.log(results);
            console.log("Results Handled!");
        };

        const query = {};
        const projection = { _id: 0, timestamp: 1 };
        const options = { limit: 2 };
        const findOptions = { ...options, projection };
        //example find
        mCollect.find(collectionName, query, findOptions, handleResults);
        console.log("Finish find!");

        //example call to disconnect
        //mCollect.endConnection();
    }, []);

    return (<>
        {documents.map((doc, index) => (<p key={index}>{JSON.stringify(doc)}</p>))}
    </>);
};

export default App;
