import ballerina/http;
import ballerina/log;
import ballerina/uuid;

service /orders on new http:Listener(port) {

    // ---------------------------------------------
    // POST /orders  -> place a new order
    // ---------------------------------------------
    resource function post .(@http:Payload NewOrderRequest req)
            returns http:Created|http:BadRequest|error {

        // Validation: no empty orders, no negative quantities
        if req.items.length() == 0 {
            return <http:BadRequest>{body: {message: "An order needs at least one item"}};
        }

        float total = 0.0;
        foreach OrderItem item in req.items {
            if item.quantity <= 0 {
                return <http:BadRequest>{body: {message: string `Invalid quantity for ${item.name}`}};
            }
            total += item.price * <float>item.quantity;
        }

        string now = nowString();
        string orderId = "ORD-" + uuid:createType4AsString().substring(0, 8).toUpperAscii();

        Order newOrder = {
            orderId: orderId,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            totalAmount: total,
            deliveryAddress: req.deliveryAddress,
            status: CREATED,
            statusHistory: [{status: CREATED, at: now, note: "Order placed"}],
            createdAt: now,
            updatedAt: now
        };

        // 1. Save to the database
        check ordersCollection->insertOne(newOrder);

        // 2. Shout it out on Kafka - Payment Service is listening!
        OrderCreatedEvent event = {
            orderId: orderId,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            totalAmount: total,
            deliveryAddress: req.deliveryAddress,
            timestamp: now
        };
        check publishEvent("orders.created", orderId, event);

        log:printInfo(string `New order ${orderId} created (N$${total})`);
        return <http:Created>{body: newOrder};
    }

    // ---------------------------------------------
    // GET /orders  or  GET /orders?customerId=CUST-001
    // ---------------------------------------------
    resource function get .(string? customerId) returns Order[]|error {
        map<json> filter = customerId is string ? {customerId: customerId} : {};
        stream<Order, error?> results = check ordersCollection->find(filter);
        return from Order ord in results select ord;
    }

    // ---------------------------------------------
    // GET /orders/ORD-1234ABCD
    // ---------------------------------------------
    resource function get [string orderId]() returns Order|http:NotFound|error {
        Order? found = check ordersCollection->findOne({orderId: orderId});
        if found is () {
            return <http:NotFound>{body: {message: string `Order ${orderId} not found`}};
        }
        return found;
    }

    // ---------------------------------------------
    // PUT /orders/ORD-1234ABCD/cancel
    // ---------------------------------------------
    resource function put [string orderId]/cancel()
            returns Order|http:NotFound|http:Conflict|error {
        Order|error result = updateOrderStatus(orderId, CANCELLED, "Cancelled by customer");

        if result is NotFoundError {
            return <http:NotFound>{body: {message: result.message()}};
        }
        if result is InvalidTransitionError {
            return <http:Conflict>{body: {message: "Too late to cancel - the kitchen is already cooking!"}};
        }
        return result;
    }
}