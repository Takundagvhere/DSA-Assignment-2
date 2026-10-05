// What we save: one record per message sent
type Notification record {
    string notificationId;
    string orderId;
    string recipientType;   // CUSTOMER, RESTAURANT or DRIVER
    string recipientId;
    string channel;         // SMS, EMAIL or PUSH
    string message;
    string sentAt;
};

// Events we listen to
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    float totalAmount;
    string timestamp;
};

type OrderStatusChangedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    string newStatus;
    string timestamp;
};

type DeliveryEvent record {
    string orderId;
    string deliveryId;
    string driverId;
    string status;
    string timestamp;
};