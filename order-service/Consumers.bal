import ballerina/log;
import ballerinax/kafka;

// ===================================================
// LISTENER 1: Payment results -> CONFIRMED or CANCELLED
// ===================================================
listener kafka:Listener paymentListener = new (kafkaBootstrap, {
    groupId: "order-service-payments",
    topics: ["payments.completed", "payments.failed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on paymentListener {
    remote function onConsumerRecord(PaymentEvent[] events) {
        foreach PaymentEvent e in events {
            OrderStatus target = e.status == "COMPLETED" ? CONFIRMED : CANCELLED;
            Order|error result = updateOrderStatus(e.orderId, target,
                    string `Payment ${e.status} (${e.paymentId})`);
            if result is error {
                log:printError("Could not apply payment event", result);
            }
        }
    }
}

// ===================================================
// LISTENER 2: Kitchen updates -> PREPARING or READY
// ===================================================
listener kafka:Listener kitchenListener = new (kafkaBootstrap, {
    groupId: "order-service-kitchen",
    topics: ["kitchen.updates"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on kitchenListener {
    remote function onConsumerRecord(KitchenEvent[] events) {
        foreach KitchenEvent e in events {
            OrderStatus target;
            if e.status == "PREPARING" {
                target = PREPARING;
            } else if e.status == "READY" {
                target = READY;
            } else {
                log:printWarn(string `Unknown kitchen status: ${e.status}`);
                continue;
            }
            Order|error result = updateOrderStatus(e.orderId, target, "Kitchen update");
            if result is error {
                log:printError("Could not apply kitchen event", result);
            }
        }
    }
}

// ===================================================
// LISTENER 3: Delivery updates -> OUT_FOR_DELIVERY or DELIVERED
// ===================================================
listener kafka:Listener deliveryListener = new (kafkaBootstrap, {
    groupId: "order-service-delivery",
    topics: ["delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on deliveryListener {
    remote function onConsumerRecord(DeliveryEvent[] events) {
        foreach DeliveryEvent e in events {
            OrderStatus target;
            if e.status == "ASSIGNED" {
                target = OUT_FOR_DELIVERY;
            } else if e.status == "DELIVERED" {
                target = DELIVERED;
            } else {
                log:printWarn(string `Unknown delivery status: ${e.status}`);
                continue;
            }
            Order|error result = updateOrderStatus(e.orderId, target,
                    string `Driver ${e.driverId}`);
            if result is error {
                log:printError("Could not apply delivery event", result);
            }
        }
    }
}