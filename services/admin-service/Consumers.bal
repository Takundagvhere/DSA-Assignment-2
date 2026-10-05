import ballerina/log;
import ballerinax/kafka;

// ---------- Orders ----------
listener kafka:Listener createdListener = new (kafkaBootstrap, {
    groupId: "admin-service-created",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on createdListener {
    remote function onConsumerRecord(OrderCreatedEvent[] events) {
        foreach OrderCreatedEvent e in events {
            error? result = saveOrder(e.orderId, e.restaurantId, e.totalAmount, "CREATED", e.timestamp);
            if result is error {
                log:printError("Could not record order", result);
            }
        }
    }
}

listener kafka:Listener statusListener = new (kafkaBootstrap, {
    groupId: "admin-service-status",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on statusListener {
    remote function onConsumerRecord(OrderStatusChangedEvent[] events) {
        foreach OrderStatusChangedEvent e in events {
            error? result = saveOrder(e.orderId, e.restaurantId, e.totalAmount, e.newStatus, e.timestamp);
            if result is error {
                log:printError("Could not record status change", result);
            }
        }
    }
}

// ---------- Deliveries ----------
listener kafka:Listener deliveryListener = new (kafkaBootstrap, {
    groupId: "admin-service-delivery",
    topics: ["delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on deliveryListener {
    remote function onConsumerRecord(DeliveryEvent[] events) {
        foreach DeliveryEvent e in events {
            error? result = saveDelivery(e);
            if result is error {
                log:printError("Could not record delivery", result);
            }
        }
    }
}

function saveOrder(string orderId, string restaurantId, float totalAmount,
        string status, string timestamp) returns error? {
    OrderRecord? existing = check ordersCollection->findOne({orderId: orderId});
    if existing is () {
        OrderRecord rec = {
            orderId: orderId,
            restaurantId: restaurantId,
            totalAmount: totalAmount,
            status: status,
            createdAt: timestamp
        };
        check ordersCollection->insertOne(rec);
        return;
    }
    if status == "CREATED" {
        return;     // never overwrite a newer status
    }
    _ = check ordersCollection->updateOne({orderId: orderId}, {set: {status: status}});
}

function saveDelivery(DeliveryEvent e) returns error? {
    DeliveryRecord? existing = check deliveriesCollection->findOne({deliveryId: e.deliveryId});

    if e.status == "ASSIGNED" {
        if existing is () {
            DeliveryRecord rec = {
                deliveryId: e.deliveryId,
                orderId: e.orderId,
                driverId: e.driverId,
                assignedAt: e.timestamp,
                deliveredAt: ""
            };
            check deliveriesCollection->insertOne(rec);
        }
    } else if e.status == "DELIVERED" {
        if existing is () {
            DeliveryRecord rec = {
                deliveryId: e.deliveryId,
                orderId: e.orderId,
                driverId: e.driverId,
                assignedAt: "",
                deliveredAt: e.timestamp
            };
            check deliveriesCollection->insertOne(rec);
        } else {
            _ = check deliveriesCollection->updateOne({deliveryId: e.deliveryId}, {
                set: {deliveredAt: e.timestamp}
            });
        }
    }
}