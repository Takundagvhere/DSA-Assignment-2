import ballerina/log;
import ballerina/time;

// ===================================================
// THE STATE MACHINE
// For each status: which statuses are allowed next?
// ===================================================
final map<OrderStatus[]> & readonly allowedTransitions = {
    CREATED: [CONFIRMED, CANCELLED],
    CONFIRMED: [PREPARING, CANCELLED],
    PREPARING: [READY],
    READY: [OUT_FOR_DELIVERY],
    OUT_FOR_DELIVERY: [DELIVERED],
    DELIVERED: [],      // final state - the end
    CANCELLED: []       // final state - the end
};

function canTransition(OrderStatus fromStatus, OrderStatus toStatus) returns boolean {
    OrderStatus[]? nextOptions = allowedTransitions[fromStatus];
    return nextOptions is OrderStatus[] && nextOptions.indexOf(toStatus) is int;
}

function nowString() returns string => time:utcToString(time:utcNow());

// ===================================================
// The ONLY function in the whole system allowed to
// change an order's status. One boss, no confusion.
// ===================================================
function updateOrderStatus(string orderId, OrderStatus newStatus, string note) returns Order|error {
    Order? current = check ordersCollection->findOne({orderId: orderId});
    if current is () {
        return error NotFoundError(string `Order ${orderId} not found`);
    }

    OrderStatus oldStatus = current.status;

    // Kafka can deliver the same event twice - if we're already
    // in that status, just ignore the duplicate
    if oldStatus == newStatus {
        log:printInfo(string `Order ${orderId} already ${newStatus} - duplicate event ignored`);
        return current;
    }

    if !canTransition(oldStatus, newStatus) {
        return error InvalidTransitionError(
            string `Cannot move order ${orderId} from ${oldStatus} to ${newStatus}`);
    }

    string now = nowString();
    StatusChange change = {status: newStatus, at: now, note: note};

    // Update MongoDB: set the new status AND add a diary entry
        // 1. Update our local copy first (add the new diary entry)
    current.status = newStatus;
    current.updatedAt = now;
    current.statusHistory.push(change);

    // 2. Save it to MongoDB: new status, new time, and the full updated diary
    _ = check ordersCollection->updateOne({orderId: orderId}, {
        set: {
            status: newStatus,
            updatedAt: now,
            statusHistory: current.statusHistory.toJson()
        }
    });

    // Tell the whole system about the change
    OrderStatusChangedEvent event = {
        orderId: orderId,
        customerId: current.customerId,
        restaurantId: current.restaurantId,
        deliveryAddress: current.deliveryAddress,
        oldStatus: oldStatus,
        newStatus: newStatus,
        totalAmount: current.totalAmount,
        timestamp: now
    };
    check publishEvent("orders.status-changed", orderId, event);

    log:printInfo(string `Order ${orderId}: ${oldStatus} -> ${newStatus}`);
    return current;
}