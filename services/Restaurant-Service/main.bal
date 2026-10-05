import ballerina/http;

service /restaurants on new http:Listener(port) {

    // POST /restaurants  -> register a restaurant
    resource function post .(@http:Payload NewRestaurantRequest req)
            returns http:Created|http:BadRequest|error {
        if req.openingHour < 0 || req.openingHour > 23 || req.closingHour < 1 || req.closingHour > 24 {
            return <http:BadRequest>{body: {message: "Opening hour must be 0-23 and closing hour 1-24"}};
        }
        Restaurant restaurant = {
            restaurantId: newId("REST-"),
            name: req.name,
            address: req.address,
            openingHour: req.openingHour,
            closingHour: req.closingHour,
            menu: [],
            createdAt: nowString()
        };
        check restaurantsCollection->insertOne(restaurant);
        return <http:Created>{body: restaurant};
    }

    // GET /restaurants  -> list all restaurants
    resource function get .() returns Restaurant[]|error {
        stream<Restaurant, error?> results = check restaurantsCollection->find();
        return from Restaurant r in results select r;
    }

    // GET /restaurants/REST-1234
    resource function get [string restaurantId]() returns Restaurant|http:NotFound|error {
        Restaurant|error r = findRestaurant(restaurantId);
        if r is NotFoundError {
            return <http:NotFound>{body: {message: r.message()}};
        }
        return r;
    }

    // GET /restaurants/REST-1234/open  -> open right now?
    resource function get [string restaurantId]/open() returns OpenStatus|http:NotFound|error {
        Restaurant|error r = findRestaurant(restaurantId);
        if r is NotFoundError {
            return <http:NotFound>{body: {message: r.message()}};
        }
        Restaurant restaurant = check r;
        return {
            restaurantId: restaurant.restaurantId,
            name: restaurant.name,
            isOpen: isOpenNow(restaurant),
            currentHour: currentHour(),
            openingHour: restaurant.openingHour,
            closingHour: restaurant.closingHour
        };
    }

    // POST /restaurants/REST-1234/menu  -> add a menu item
    resource function post [string restaurantId]/menu(@http:Payload NewMenuItemRequest req)
            returns http:Created|http:NotFound|http:BadRequest|error {
        if req.price <= 0.0 || req.stock < 0 {
            return <http:BadRequest>{body: {message: "Price must be above 0 and stock cannot be negative"}};
        }
        Restaurant|error r = findRestaurant(restaurantId);
        if r is NotFoundError {
            return <http:NotFound>{body: {message: r.message()}};
        }
        Restaurant restaurant = check r;

        MenuItem item = {
            itemId: newId("ITEM-"),
            name: req.name,
            price: req.price,
            stock: req.stock,
            available: req.stock > 0
        };
        restaurant.menu.push(item);     // Ballerina's own list push - totally fine
        check saveMenu(restaurantId, restaurant.menu);
        return <http:Created>{body: item};
    }

    // GET /restaurants/REST-1234/menu
    resource function get [string restaurantId]/menu() returns MenuItem[]|http:NotFound|error {
        Restaurant|error r = findRestaurant(restaurantId);
        if r is NotFoundError {
            return <http:NotFound>{body: {message: r.message()}};
        }
        Restaurant restaurant = check r;
        return restaurant.menu;
    }

    // PUT /restaurants/REST-1234/menu/ITEM-5678/stock  -> restock
    resource function put [string restaurantId]/menu/[string itemId]/stock(@http:Payload StockUpdateRequest req)
            returns MenuItem|http:NotFound|http:BadRequest|error {
        if req.stock < 0 {
            return <http:BadRequest>{body: {message: "Stock cannot be negative"}};
        }
        Restaurant|error r = findRestaurant(restaurantId);
        if r is NotFoundError {
            return <http:NotFound>{body: {message: r.message()}};
        }
        Restaurant restaurant = check r;

        foreach MenuItem item in restaurant.menu {
            if item.itemId == itemId {
                item.stock = req.stock;
                item.available = req.stock > 0;
                check saveMenu(restaurantId, restaurant.menu);
                return item;
            }
        }
        return <http:NotFound>{body: {message: string `Item ${itemId} not found`}};
    }

    // GET /restaurants/REST-1234/kitchen  -> the kitchen queue
    resource function get [string restaurantId]/kitchen() returns KitchenOrder[]|error {
        stream<KitchenOrder, error?> results = check kitchenCollection->find({restaurantId: restaurantId});
        return from KitchenOrder ko in results
            where ko.status == WAITING || ko.status == PREPARING
            select ko;
    }

    // PUT /restaurants/REST-1234/kitchen/ORD-9999/preparing  -> start cooking
    resource function put [string restaurantId]/kitchen/[string orderId]/preparing()
            returns KitchenOrder|http:NotFound|http:Conflict|error {
        return kitchenResponse(moveKitchenOrder(restaurantId, orderId, WAITING, PREPARING));
    }

    // PUT /restaurants/REST-1234/kitchen/ORD-9999/ready  -> food's ready
    resource function put [string restaurantId]/kitchen/[string orderId]/ready()
            returns KitchenOrder|http:NotFound|http:Conflict|error {
        return kitchenResponse(moveKitchenOrder(restaurantId, orderId, PREPARING, READY));
    }
}

// Turns our error types into the right HTTP codes
function kitchenResponse(KitchenOrder|error result)
        returns KitchenOrder|http:NotFound|http:Conflict|error {
    if result is NotFoundError {
        return <http:NotFound>{body: {message: result.message()}};
    }
    if result is ConflictError {
        return <http:Conflict>{body: {message: result.message()}};
    }
    return result;
}
