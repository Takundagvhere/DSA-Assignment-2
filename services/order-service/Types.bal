// ===================================================
// ORDER STATUS - every state an order can be in
// ===================================================
enum OrderStatus {
    CREATED,
    CONFIRMED,
    PREPARING,
    READY,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED
}

// ===================================================
// DATA WE SAVE IN MONGODB
// ===================================================
type OrderItem record {
    string itemId;
    string name;
    int quantity;
    float price;
};

// One entry in the order's "diary" of status changes
type StatusChange record {
    OrderStatus status;
    string at;
    string note;
};

type Order record {
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    float totalAmount;
    string deliveryAddress;
    OrderStatus status;
    StatusChange[] statusHistory;
    string createdAt;
    string updatedAt;
};

// ===================================================
// WHAT THE CUSTOMER SENDS WHEN ORDERING
// {| |} means "closed": extra, unknown fields get rejected
// ===================================================
type NewOrderRequest record {|
    string customerId;
    string restaurantId;
    string deliveryAddress;
    OrderItem[] items;
|};

// ===================================================
// EVENTS WE SEND TO KAFKA
// ===================================================
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    float totalAmount;
    string deliveryAddress;
    string timestamp;
};

type OrderStatusChangedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    string deliveryAddress;
    OrderStatus oldStatus;
    OrderStatus newStatus;
    float totalAmount;
    string timestamp;
};

// ===================================================
// EVENTS WE RECEIVE FROM OTHER SERVICES
// ===================================================
type PaymentEvent record {
    string orderId;
    string paymentId;
    string status;      // "COMPLETED" or "FAILED"
    float amount;
    string timestamp;
};

type KitchenEvent record {
    string orderId;
    string restaurantId;
    string status;      // "PREPARING" or "READY"
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
// OUR OWN ERROR TYPES (so we can return the right HTTP code)
// ===================================================
type NotFoundError distinct error;
type InvalidTransitionError distinct error;

