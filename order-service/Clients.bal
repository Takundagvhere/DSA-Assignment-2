import ballerinax/kafka;
import ballerinax/mongodb;

// ===================================================
// SETTINGS - "configurable" means we can change these
// later (for Docker) without touching the code
// ===================================================
configurable int port = 8083;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";

// ===================================================
// KAFKA PRODUCER - our "megaphone" for sending events
// ===================================================
final kafka:Producer eventProducer = check new (kafkaBootstrap);

// ===================================================
// MONGODB - each service gets its OWN database
// ===================================================
final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection ordersCollection = check getOrdersCollection();

function getOrdersCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("order_db");
    return db->getCollection("orders");
}

// ===================================================
// Sends an event to a Kafka topic.
// The orderId is the KEY, so every event for the same order
// goes to the same partition and stays in the right order.
// ===================================================
function publishEvent(string topic, string orderId, anydata event) returns error? {
    check eventProducer->send({
        topic: topic,
        'key: orderId.toBytes(),
        value: event.toJsonString().toBytes()
    });
}