import ballerina/log;
import ballerina/time;
import ballerina/uuid;

function nowString() returns string => time:utcToString(time:utcNow());

function newId(string prefix) returns string =>
    prefix + uuid:createType4AsString().substring(0, 8).toUpperAscii();

// ===================================================
// 1. AN ORDER IS READY -> create a delivery job
// ===================================================
function createDelivery(OrderStatusChangedEvent e) returns error? {
    // Duplicate protection: one delivery per order
    Delivery? existing = check deliveriesCollection->findOne({orderId: e.orderId});
    if existing is Delivery {
        log:printInfo(string `Delivery for ${e.orderId} already exists - duplicate ignored`);
        return;
    }

    Delivery delivery = {
        deliveryId: newId("DEL-"),
        orderId: e.orderId,
        restaurantId: e.restaurantId,
        customerId: e.customerId,
        deliveryAddress: e.deliveryAddress,
        driverId: "",
        status: WAITING_FOR_DRIVER,
        createdAt: nowString(),
        assignedAt: "",
        deliveredAt: ""
    };
    check deliveriesCollection->insertOne(delivery);
    log:printInfo(string `New delivery job ${delivery.deliveryId} for order ${e.orderId}`);

    check assignWaitingDeliveries();
}

// ===================================================
// 2. MATCH WAITING JOBS WITH FREE DRIVERS
// ===================================================
function assignWaitingDeliveries() returns error? {
    while true {
        Delivery? waiting = check deliveriesCollection->findOne({status: WAITING_FOR_DRIVER});
        if waiting is () {
            return;     // no jobs waiting
        }

        Driver? driver = check driversCollection->findOne({status: AVAILABLE});
        if driver is () {
            log:printInfo("No free drivers right now - deliveries are waiting in the queue");
            return;
        }

        check assignDriver(waiting, driver);
    }
}

function assignDriver(Delivery delivery, Driver driver) returns error? {
    string now = nowString();

    _ = check driversCollection->updateOne({driverId: driver.driverId}, {
        set: {status: BUSY}
    });

    _ = check deliveriesCollection->updateOne({deliveryId: delivery.deliveryId}, {
        set: {driverId: driver.driverId, status: ASSIGNED, assignedAt: now}
    });

    DeliveryEvent event = {
        orderId: delivery.orderId,
        deliveryId: delivery.deliveryId,
        driverId: driver.driverId,
        status: "ASSIGNED",
        timestamp: now
    };
    check publishEvent("delivery.assigned", delivery.orderId, event);

    log:printInfo(string `Driver ${driver.name} assigned to order ${delivery.orderId}`);
}

// ===================================================
// 3. THE DRIVER DELIVERED THE FOOD
// ===================================================
function completeDelivery(string deliveryId) returns Delivery|error {
    Delivery? d = check deliveriesCollection->findOne({deliveryId: deliveryId});
    if d is () {
        return error NotFoundError(string `Delivery ${deliveryId} not found`);
    }
    if d.status != ASSIGNED {
        return error ConflictError(
            string `Delivery ${deliveryId} is ${d.status} - only ASSIGNED deliveries can be completed`);
    }

    string now = nowString();

    _ = check deliveriesCollection->updateOne({deliveryId: deliveryId}, {
        set: {status: DELIVERED, deliveredAt: now}
    });

    Driver? driver = check driversCollection->findOne({driverId: d.driverId});
    if driver is Driver {
        _ = check driversCollection->updateOne({driverId: driver.driverId}, {
            set: {status: AVAILABLE, completedDeliveries: driver.completedDeliveries + 1}
        });
    }

    DeliveryEvent event = {
        orderId: d.orderId,
        deliveryId: deliveryId,
        driverId: d.driverId,
        status: "DELIVERED",
        timestamp: now
    };
    check publishEvent("delivery.completed", d.orderId, event);
    log:printInfo(string `Order ${d.orderId} delivered!`);

    d.status = DELIVERED;
    d.deliveredAt = now;

    // The driver is free again - anyone else waiting?
    check assignWaitingDeliveries();
    return d;
}