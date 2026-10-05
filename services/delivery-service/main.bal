import ballerina/http;

service / on new http:Listener(port) {

    // ========== DRIVERS ==========

    // POST /drivers  -> a new driver signs up
    resource function post drivers(@http:Payload NewDriverRequest req) returns http:Created|error {
        Driver driver = {
            driverId: newId("DRV-"),
            name: req.name,
            phone: req.phone,
            vehicle: req.vehicle,
            status: AVAILABLE,
            completedDeliveries: 0,
            createdAt: nowString()
        };
        check driversCollection->insertOne(driver);

        check assignWaitingDeliveries();

        Driver? latest = check driversCollection->findOne({driverId: driver.driverId});
        return <http:Created>{body: latest is Driver ? latest : driver};
    }

    // GET /drivers
    resource function get drivers() returns Driver[]|error {
        stream<Driver, error?> results = check driversCollection->find();
        return from Driver d in results select d;
    }

    // PUT /drivers/DRV-1234/status  -> go AVAILABLE or OFFLINE
    resource function put drivers/[string driverId]/status(@http:Payload DriverStatusRequest req)
            returns Driver|http:NotFound|http:BadRequest|http:Conflict|error {
        Driver? driver = check driversCollection->findOne({driverId: driverId});
        if driver is () {
            return <http:NotFound>{body: {message: string `Driver ${driverId} not found`}};
        }
        if driver.status == BUSY {
            return <http:Conflict>{body: {message: "Finish your current delivery first!"}};
        }

        DriverStatus newStatus;
        if req.status == "AVAILABLE" {
            newStatus = AVAILABLE;
        } else if req.status == "OFFLINE" {
            newStatus = OFFLINE;
        } else {
            return <http:BadRequest>{body: {message: "Status must be AVAILABLE or OFFLINE"}};
        }

        _ = check driversCollection->updateOne({driverId: driverId}, {set: {status: newStatus}});

        if newStatus == AVAILABLE {
            check assignWaitingDeliveries();
        }

        Driver? latest = check driversCollection->findOne({driverId: driverId});
        return latest is Driver ? latest : driver;
    }

    // GET /drivers/DRV-1234/current  -> the driver's current job
    resource function get drivers/[string driverId]/current() returns Delivery|http:NotFound|error {
        Delivery? job = check deliveriesCollection->findOne({driverId: driverId, status: ASSIGNED});
        if job is () {
            return <http:NotFound>{body: {message: string `Driver ${driverId} has no active delivery`}};
        }
        return job;
    }

    // ========== DELIVERIES ==========

    // GET /deliveries  (optional ?status=... or ?orderId=...)
    resource function get deliveries(string? orderId, string? status) returns Delivery[]|error {
        map<json> filter = {};
        if orderId is string {
            filter["orderId"] = orderId;
        }
        if status is string {
            filter["status"] = status;
        }
        stream<Delivery, error?> results = check deliveriesCollection->find(filter);
        return from Delivery d in results select d;
    }

    // GET /deliveries/DEL-1234
    resource function get deliveries/[string deliveryId]() returns Delivery|http:NotFound|error {
        Delivery? d = check deliveriesCollection->findOne({deliveryId: deliveryId});
        if d is () {
            return <http:NotFound>{body: {message: string `Delivery ${deliveryId} not found`}};
        }
        return d;
    }

    // POST /deliveries/DEL-1234/complete  -> the driver presses "Delivered"
    resource function post deliveries/[string deliveryId]/complete()
            returns Delivery|http:NotFound|http:Conflict|error {
        Delivery|error result = completeDelivery(deliveryId);
        if result is NotFoundError {
            return <http:NotFound>{body: {message: result.message()}};
        }
        if result is ConflictError {
            return <http:Conflict>{body: {message: result.message()}};
        }
        return result;
    }
}