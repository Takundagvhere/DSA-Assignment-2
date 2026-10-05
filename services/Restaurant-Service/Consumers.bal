import ballerina/log;
import ballerinax/kafka;

// LISTENER 1: new orders -> remember the items
listener kafka:Listener newOrderListener = new (kafkaBootstrap, {
    groupId: "restaurant-service-orders",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on newOrderListener {
    remote function onConsumerRecord(OrderCreatedEvent[] events) {
        foreach OrderCreatedEvent e in events {
            error? result = receiveOrder(e);
            if result is error {
                log:printError(string `Could not store order ${e.orderId}`, result);
            }
        }
    }
}

// LISTENER 2: status changes -> CONFIRMED or CANCELLED
listener kafka:Listener statusListener = new (kafkaBootstrap, {
    groupId: "restaurant-service-status",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on statusListener {
    remote function onConsumerRecord(OrderStatusChangedEvent[] events) {
        foreach OrderStatusChangedEvent e in events {
            error? result = ();
            if e.newStatus == "CONFIRMED" {
                result = confirmOrder(e.orderId);
            } else if e.newStatus == "CANCELLED" {
                result = cancelOrder(e.orderId);
            }
            if result is error {
                log:printError(string `Could not handle ${e.newStatus} for ${e.orderId}`, result);
            }
        }
    }
}