// ===================================================
// STATUSES
// ===================================================
enum DriverStatus {
    AVAILABLE,      // online and free
    BUSY,           // on a delivery
    OFFLINE         // logged off
}

enum DeliveryStatus {
    WAITING_FOR_DRIVER,
    ASSIGNED,
    DELIVERED
}

// ===================================================
// DATA WE SAVE IN MONGODB
// ===================================================
type Driver record {
    string driverId;
    string name;
    string phone;
    string vehicle;
    DriverStatus status;
    int completedDeliveries;
    string createdAt;
};

// An empty string "" means "not yet" (no driver yet, not delivered yet)
type Delivery record {
    string deliveryId;
    string orderId;
    string restaurantId;
    string customerId;
    string deliveryAddress;
    string driverId;
    DeliveryStatus status;
    string createdAt;
    string assignedAt;
    string deliveredAt;
};

// ===================================================
// WHAT CLIENTS SEND US
// ===================================================
type NewDriverRequest record {|
    string name;
    string phone;
    string vehicle;
|};

type DriverStatusRequest record {|
    string status;      // "AVAILABLE" or "OFFLINE"
|};

// ===================================================
// EVENTS
// ===================================================
type OrderStatusChangedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    string deliveryAddress;
    string oldStatus;
    string newStatus;
    string timestamp;
};

type DeliveryEvent record {
    string orderId;
    string deliveryId;
    string driverId;
    string status;      // "ASSIGNED" or "DELIVERED"
    string timestamp;
};

// ===================================================
// OUR ERROR TYPES
// ===================================================
type NotFoundError distinct error;
type ConflictError distinct error;