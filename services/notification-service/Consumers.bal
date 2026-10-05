import ballerina/log;
import ballerinax/kafka;

// ---------- 1. New orders -> tell the customer ----------
listener kafka:Listener createdListener = new (kafkaBootstrap, {
    groupId: "notification-service-created",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on createdListener {
    remote function onConsumerRecord(OrderCreatedEvent[] events) {
        foreach OrderCreatedEvent e in events {
            error? result = notify(e.orderId, "CREATED", "CUSTOMER", e.customerId,
                    string `We received your order ${e.orderId} (N$${e.totalAmount}). Waiting for payment.`,
                    ["SMS", "EMAIL"]);
            if result is error {
                log:printError("Notification failed", result);
            }
        }
    }
}

// ---------- 2. Status changes -> customer and restaurant ----------
listener kafka:Listener statusListener = new (kafkaBootstrap, {
    groupId: "notification-service-status",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on statusListener {
    remote function onConsumerRecord(OrderStatusChangedEvent[] events) {
        foreach OrderStatusChangedEvent e in events {
            error? result = handleStatusChange(e);
            if result is error {
                log:printError("Notification failed", result);
            }
        }
    }
}

// ---------- 3. Driver assigned -> tell the driver ----------
listener kafka:Listener deliveryListener = new (kafkaBootstrap, {
    groupId: "notification-service-delivery",
    topics: ["delivery.assigned"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on deliveryListener {
    remote function onConsumerRecord(DeliveryEvent[] events) {
        foreach DeliveryEvent e in events {
            error? result = notify(e.orderId, "ASSIGNED", "DRIVER", e.driverId,
                    string `New delivery job ${e.deliveryId}: pick up order ${e.orderId}`,
                    ["PUSH", "SMS"]);
            if result is error {
                log:printError("Notification failed", result);
            }
        }
    }
}

// ===================================================
// Who gets told what, for each status
// ===================================================
function handleStatusChange(OrderStatusChangedEvent e) returns error? {
    string? customerMsg = customerMessage(e.orderId, e.newStatus);
    if customerMsg is string {
        check notify(e.orderId, e.newStatus, "CUSTOMER", e.customerId, customerMsg, ["SMS", "EMAIL"]);
    }

    if e.newStatus == "CONFIRMED" {
        check notify(e.orderId, e.newStatus, "RESTAURANT", e.restaurantId,
                string `New paid order ${e.orderId} - please start preparing`, ["PUSH"]);
    } else if e.newStatus == "CANCELLED" {
        check notify(e.orderId, e.newStatus, "RESTAURANT", e.restaurantId,
                string `Order ${e.orderId} was cancelled`, ["PUSH"]);
    }
}

function customerMessage(string orderId, string status) returns string? {
    match status {
        "CONFIRMED" => {
            return string `Payment received! Order ${orderId} is confirmed.`;
        }
        "PREPARING" => {
            return string `The kitchen is cooking your order ${orderId}.`;
        }
        "READY" => {
            return string `Your order ${orderId} is ready - finding you a driver.`;
        }
        "OUT_FOR_DELIVERY" => {
            return string `Your driver is on the way with order ${orderId}!`;
        }
        "DELIVERED" => {
            return string `Order ${orderId} delivered. Enjoy your meal!`;
        }
        "CANCELLED" => {
            return string `Sorry, order ${orderId} was cancelled.`;
        }
    }
    return ();
}