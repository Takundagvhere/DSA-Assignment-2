// ===================================================
// KITCHEN STATUS - the kitchen's own mini state machine
// ===================================================
enum KitchenStatus {
    PENDING_PAYMENT,   // order arrived, waiting for payment
    WAITING,           // paid, in the queue
    PREPARING,         // chef is cooking
    READY,             // food is ready for the driver
    CANCELLED
}

// ===================================================
// DATA WE SAVE IN MONGODB
// ===================================================
type MenuItem record {
    string itemId;
    string name;
    float price;
    int stock;
    boolean available;
};

type Restaurant record {
    string restaurantId;
    string name;
    string address;
    int openingHour;    // 0-23, e.g. 10 = 10:00
    int closingHour;    // 1-24, e.g. 22 = 22:00
    MenuItem[] menu;
    string createdAt;
};

type OrderItem record {
    string itemId;
    string name;
    int quantity;
    float price;
};

type KitchenOrder record {
    string orderId;
    string restaurantId;
    string customerId;
    OrderItem[] items;
    KitchenStatus status;
    string receivedAt;
    string updatedAt;
};

// ===================================================
// WHAT CLIENTS SEND US
// ===================================================
type NewRestaurantRequest record {|
    string name;
    string address;
    int openingHour;
    int closingHour;
|};

type NewMenuItemRequest record {|
    string name;
    float price;
    int stock;
|};

type StockUpdateRequest record {|
    int stock;
|};

// What GET /restaurants/{id}/open returns
type OpenStatus record {|
    string restaurantId;
    string name;
    boolean isOpen;
    int currentHour;
    int openingHour;
    int closingHour;
|};

// ===================================================
// EVENTS WE RECEIVE
// ===================================================
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    string timestamp;
};

type OrderStatusChangedEvent record {
    string orderId;
    string restaurantId;
    string oldStatus;
    string newStatus;
    string timestamp;
};

// ===================================================
// EVENT WE SEND
// ===================================================
type KitchenEvent record {
    string orderId;
    string restaurantId;
    string status;      // "PREPARING" or "READY"
    string timestamp;
};

// ===================================================
// OUR ERROR TYPES
// ===================================================
type NotFoundError distinct error;
type ConflictError distinct error;