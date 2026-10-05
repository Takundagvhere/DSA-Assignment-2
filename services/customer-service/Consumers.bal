import ballerina/log;
import ballerinax/kafka;

listener kafka:Listener createdListener = new (kafkaBootstrap, {
    groupId: "customer-service-created",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on createdListener {
    remote function onConsumerRecord(OrderCreatedEvent[] events) {
        foreach OrderCreatedEvent e in events {
            error? result = saveHistory(e.orderId, e.customerId, e.restaurantId,
                    e.totalAmount, "CREATED", e.timestamp);
            if result is error {
                log:printError("Could not save order history", result);
            }
        }
    }
}

listener kafka:Listener statusListener = new (kafkaBootstrap, {
    groupId: "customer-service-status",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on statusListener {
    remote function onConsumerRecord(OrderStatusChangedEvent[] events) {
        foreach OrderStatusChangedEvent e in events {
            error? result = saveHistory(e.orderId, e.customerId, e.restaurantId,
                    e.totalAmount, e.newStatus, e.timestamp);
            if result is error {
                log:printError("Could not update order history", result);
            }
        }
    }
}

// Adds the order to the history, or updates its status
function saveHistory(string orderId, string customerId, string restaurantId,
        float totalAmount, string status, string timestamp) returns error? {
    OrderHistoryEntry? existing = check historyCollection->findOne({orderId: orderId});
    if existing is () {
        OrderHistoryEntry entry = {
            orderId: orderId,
            customerId: customerId,
            restaurantId: restaurantId,
            totalAmount: totalAmount,
            status: status,
            placedAt: timestamp,
            lastUpdated: timestamp
        };
        check historyCollection->insertOne(entry);
        return;
    }
    // The two listeners run separately, so a late "CREATED"
    // must never overwrite a newer status like DELIVERED
    if status == "CREATED" {
        return;
    }
    _ = check historyCollection->updateOne({orderId: orderId}, {
        set: {status: status, lastUpdated: timestamp}
    });
}