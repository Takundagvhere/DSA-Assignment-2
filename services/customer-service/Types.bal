type Address record {
    string addressId;
    string label;       // e.g. "Home", "Work"
    string street;
    string city;
};

type Customer record {
    string customerId;
    string name;
    string email;
    string phone;
    Address[] addresses;
    string createdAt;
};

type NewCustomerRequest record {|
    string name;
    string email;
    string phone;
|};

type NewAddressRequest record {|
    string label;
    string street;
    string city;
|};

// Our own copy of each order, built from Kafka events
type OrderHistoryEntry record {
    string orderId;
    string customerId;
    string restaurantId;
    float totalAmount;
    string status;
    string placedAt;
    string lastUpdated;
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
    float totalAmount;
    string timestamp;
};