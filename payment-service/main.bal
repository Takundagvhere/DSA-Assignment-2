import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/random;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

// ===================================================
// SETTINGS
// ===================================================
configurable int port = 8084;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";
configurable float successRate = 0.9;   // 90% of payments succeed

// ===================================================
// CONNECTIONS
// ===================================================
final kafka:Producer eventProducer = check new (kafkaBootstrap);
final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection paymentsCollection = check getPaymentsCollection();

function getPaymentsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("payment_db");
    return db->getCollection("payments");
}

// ===================================================
// KAFKA LISTENER: waits for new orders
// ===================================================
listener kafka:Listener orderListener = new (kafkaBootstrap, {
    groupId: "payment-service",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on orderListener {
    remote function onConsumerRecord(OrderCreatedEvent[] events) {
        foreach OrderCreatedEvent e in events {
            error? result = processPayment(e);
            if result is error {
                log:printError(string `Payment failed for ${e.orderId}`, result);
            }
        }
    }
}

// ===================================================
// THE FAKE PAYMENT PROCESS
// ===================================================
function processPayment(OrderCreatedEvent e) returns error? {
    // Idempotency: never charge the same order twice!
    Payment? existing = check paymentsCollection->findOne({orderId: e.orderId});
    if existing is Payment {
        log:printInfo(string `Order ${e.orderId} already paid - skipping duplicate`);
        return;
    }

    log:printInfo(string `Processing N$${e.totalAmount} for order ${e.orderId}...`);
    runtime:sleep(2);   // pretend we're waiting for the bank

    boolean approved = random:createDecimal() < successRate;
    string now = time:utcToString(time:utcNow());

    Payment payment = {
        paymentId: "PAY-" + uuid:createType4AsString().substring(0, 8).toUpperAscii(),
        orderId: e.orderId,
        customerId: e.customerId,
        amount: e.totalAmount,
        status: approved ? "COMPLETED" : "FAILED",
        paymentMethod: "CARD",
        timestamp: now
    };
    check paymentsCollection->insertOne(payment);

    PaymentEvent event = {
        orderId: payment.orderId,
        paymentId: payment.paymentId,
        status: payment.status,
        amount: payment.amount,
        timestamp: now
    };
    string topic = approved ? "payments.completed" : "payments.failed";

    check eventProducer->send({
        topic: topic,
        'key: e.orderId.toBytes(),
        value: event.toJsonString().toBytes()
    });

    log:printInfo(string `Payment ${payment.status} for ${e.orderId}`);
}

// ===================================================
// REST API - so we can look at payments
// ===================================================
service /payments on new http:Listener(port) {

    // GET /payments
    resource function get .() returns Payment[]|error {
        stream<Payment, error?> results = check paymentsCollection->find();
        return from Payment p in results select p;
    }

    // GET /payments/ORD-1234ABCD
    resource function get [string orderId]() returns Payment|http:NotFound|error {
        Payment? found = check paymentsCollection->findOne({orderId: orderId});
        if found is () {
            return <http:NotFound>{body: {message: string `No payment for ${orderId}`}};
        }
        return found;
    }
}