import ballerinax/kafka;
import ballerinax/mongodb;

configurable int port = 8085;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";

final kafka:Producer eventProducer = check new (kafkaBootstrap);

final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection driversCollection = check getCollection("drivers");
final mongodb:Collection deliveriesCollection = check getCollection("deliveries");

function getCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("delivery_db");
    return db->getCollection(name);
}

function publishEvent(string topic, string orderId, anydata event) returns error? {
    check eventProducer->send({
        topic: topic,
        'key: orderId.toBytes(),
        value: event.toJsonString().toBytes()
    });
}