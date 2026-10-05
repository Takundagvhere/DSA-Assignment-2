import ballerina/log;
import ballerina/time;
import ballerina/uuid;

// ===================================================
// SMALL HELPERS
// ===================================================
function nowString() returns string => time:utcToString(time:utcNow());

function newId(string prefix) returns string =>
    prefix + uuid:createType4AsString().substring(0, 8).toUpperAscii();

// The current hour in local (Windhoek) time
function currentHour() returns int {
    time:Utc localNow = time:utcAddSeconds(time:utcNow(), <decimal>(utcOffsetHours * 3600));
    time:Civil civil = time:utcToCivil(localNow);
    return civil.hour;
}

function isOpenNow(Restaurant r) returns boolean {
    int hour = currentHour();
    if r.openingHour <= r.closingHour {
        // Normal day, e.g. 10:00 to 22:00
        return hour >= r.openingHour && hour < r.closingHour;
    }
    // Overnight, e.g. 18:00 to 02:00
    return hour >= r.openingHour || hour < r.closingHour;
}

// ===================================================
// RESTAURANT HELPERS
// ===================================================
function findRestaurant(string restaurantId) returns Restaurant|error {
    Restaurant? r = check restaurantsCollection->findOne({restaurantId: restaurantId});
    if r is () {
        return error NotFoundError(string `Restaurant ${restaurantId} not found`);
    }
    return r;
}

function saveMenu(string restaurantId, MenuItem[] menu) returns error? {
    _ = check restaurantsCollection->updateOne({restaurantId: restaurantId}, {
        set: {menu: menu.toJson()}
    });
}

// direction -1 = take stock out, +1 = put stock back
function adjustStock(KitchenOrder ko, int direction) returns error? {
    Restaurant r = check findRestaurant(ko.restaurantId);
    foreach OrderItem orderedItem in ko.items {
        foreach MenuItem item in r.menu {
            if item.itemId == orderedItem.itemId {
                item.stock = int:max(0, item.stock + direction * orderedItem.quantity);
                item.available = item.stock > 0;
            }
        }
    }
    check saveMenu(r.restaurantId, r.menu);
}

// ===================================================
// KITCHEN: reacting to Kafka events
// ===================================================

// A new order arrived - remember its items, wait for payment
function receiveOrder(OrderCreatedEvent e) returns error? {
    KitchenOrder? existing = check kitchenCollection->findOne({orderId: e.orderId});
    if existing is KitchenOrder {
        log:printInfo(string `Order ${e.orderId} already received - duplicate ignored`);
        return;
    }
    string now = nowString();
    KitchenOrder ko = {
        orderId: e.orderId,
        restaurantId: e.restaurantId,
        customerId: e.customerId,
        items: e.items,
        status: PENDING_PAYMENT,
        receivedAt: now,
        updatedAt: now
    };
    check kitchenCollection->insertOne(ko);
    log:printInfo(string `Order ${e.orderId} received - waiting for payment`);
}

// Payment went through - into the kitchen queue, stock goes down
function confirmOrder(string orderId) returns error? {
    KitchenOrder? ko = check kitchenCollection->findOne({orderId: orderId});
    if ko is () {
        return error NotFoundError(string `Kitchen never received order ${orderId}`);
    }
    if ko.status != PENDING_PAYMENT {
        return;     // already handled - duplicate event
    }
    _ = check kitchenCollection->updateOne({orderId: orderId}, {
        set: {status: WAITING, updatedAt: nowString()}
    });
    check adjustStock(ko, -1);
    log:printInfo(string `Order ${orderId} paid - added to the kitchen queue`);
}

// Order was cancelled - stop it, and give the stock back if we took it
function cancelOrder(string orderId) returns error? {
    KitchenOrder? ko = check kitchenCollection->findOne({orderId: orderId});
    if ko is () {
        return;
    }
    KitchenStatus previous = ko.status;
    if previous != PENDING_PAYMENT && previous != WAITING {
        log:printWarn(string `Order ${orderId} cancelled, but the kitchen is already ${previous}`);
        return;
    }
    _ = check kitchenCollection->updateOne({orderId: orderId}, {
        set: {status: CANCELLED, updatedAt: nowString()}
    });
    if previous == WAITING {
        check adjustStock(ko, 1);   // put the stock back on the shelf
    }
    log:printInfo(string `Order ${orderId} cancelled in the kitchen`);
}

// ===================================================
// KITCHEN: staff pressing buttons
// ===================================================
function moveKitchenOrder(string restaurantId, string orderId,
        KitchenStatus fromStatus, KitchenStatus toStatus) returns KitchenOrder|error {

    KitchenOrder? ko = check kitchenCollection->findOne({orderId: orderId, restaurantId: restaurantId});
    if ko is () {
        return error NotFoundError(string `Order ${orderId} is not in this kitchen`);
    }
    if ko.status != fromStatus {
        return error ConflictError(
            string `Order ${orderId} is ${ko.status} - it must be ${fromStatus} first`);
    }

    // No cooking when the restaurant is closed!
    if toStatus == PREPARING {
        Restaurant r = check findRestaurant(restaurantId);
        if !isOpenNow(r) {
            return error ConflictError(string `${r.name} is closed right now`);
        }
    }

    string now = nowString();
    _ = check kitchenCollection->updateOne({orderId: orderId}, {
        set: {status: toStatus, updatedAt: now}
    });
    ko.status = toStatus;
    ko.updatedAt = now;

    KitchenEvent event = {
        orderId: orderId,
        restaurantId: restaurantId,
        status: toStatus,
        timestamp: now
    };
    check publishEvent("kitchen.updates", orderId, event);

    log:printInfo(string `Kitchen: order ${orderId} is now ${toStatus}`);
    return ko;
}