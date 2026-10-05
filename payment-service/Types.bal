// What we receive from the Order Service.
// We only list the fields we need - the record is "open",
// so any extra fields are simply accepted and ignored.
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    float totalAmount;
    string timestamp;
};

// What we save in MongoDB
type Payment record {
    string paymentId;
    string orderId;
    string customerId;
    float amount;
    string status;          // "COMPLETED" or "FAILED"
    string paymentMethod;
    string timestamp;
};

// What we send back to Kafka
type PaymentEvent record {
    string orderId;
    string paymentId;
    string status;
    float amount;
    string timestamp;
};