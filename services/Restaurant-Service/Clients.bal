import ballerinax/kafka;
import ballerinax/mongodb;

// ===================================================
// SETTINGS
// ===================================================
configurable int port = 8082;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";
configurable int utcOffsetHours = 2;    // Namibia is UTC+2

// ===================================================
// KAFKA PRODUCER
// ===================================================
final kafka:Producer eventProducer = check new (kafkaBootstrap);

// ===================================================
// MONGODB - our own database with two collections
// ===================================================
final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection restaurantsCollection = check getCollection("restaurants");
final mongodb:Collection kitchenCollection = check getCollection("kitchen_orders");

function getCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("restaurant_db");
    return db->getCollection(name);
}

// Same as in the Order Service: orderId is the key
function publishEvent(string topic, string orderId, anydata event) returns error? {
    check eventProducer->send({
        topic: topic,
        'key: orderId.toBytes(),
        value: event.toJsonString().toBytes()
    });
}