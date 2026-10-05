// Our own copies, built from events
type OrderRecord record {
    string orderId;
    string restaurantId;
    float totalAmount;
    string status;
    string createdAt;
};

type DeliveryRecord record {
    string deliveryId;
    string orderId;
    string driverId;
    string assignedAt;
    string deliveredAt;     // "" = not delivered yet
};

// What the reports return
type RestaurantReport record {|
    string restaurantId;
    int totalOrders;
    int delivered;
    int cancelled;
    float revenue;
|};

type DriverReport record {|
    string driverId;
    int completedDeliveries;
    float averageMinutes;
|};

type DeliveryReport record {|
    int totalDeliveries;
    int completed;
    int inProgress;
    float averageMinutes;
    DriverReport[] drivers;
|};

// Events we listen to
type OrderCreatedEvent record {
    string orderId;
    string restaurantId;
    float totalAmount;
    string timestamp;
};

type OrderStatusChangedEvent record {
    string orderId;
    string restaurantId;
    string newStatus;
    float totalAmount;
    string timestamp;
};

type DeliveryEvent record {
    string orderId;
    string deliveryId;
    string driverId;
    string status;      // "ASSIGNED" or "DELIVERED"
    string timestamp;
};