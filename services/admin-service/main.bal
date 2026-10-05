import ballerina/http;
import ballerina/time;
import ballerinax/mongodb;

configurable int port = 8087;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";

final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection ordersCollection = check getCollection("orders");
final mongodb:Collection deliveriesCollection = check getCollection("deliveries");

function getCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("admin_db");
    return db->getCollection(name);
}

function minutesBetween(string fromTime, string toTime) returns float|error {
    time:Utc startTime = check time:utcFromString(fromTime);
    time:Utc endTime = check time:utcFromString(toTime);
    decimal seconds = time:utcDiffSeconds(endTime, startTime);
    return <float>seconds / 60.0;
}

service /reports on new http:Listener(port) {

    // GET /reports/summary  -> how many orders in each status
    resource function get summary() returns map<int>|error {
        stream<OrderRecord, error?> results = check ordersCollection->find();
        OrderRecord[] orders = check from OrderRecord o in results select o;

        map<int> counts = {};
        foreach OrderRecord o in orders {
            counts[o.status] = (counts[o.status] ?: 0) + 1;
        }
        return counts;
    }

    // GET /reports/restaurants  -> stats per restaurant
    resource function get restaurants() returns RestaurantReport[]|error {
        stream<OrderRecord, error?> results = check ordersCollection->find();
        OrderRecord[] orders = check from OrderRecord o in results select o;

        map<RestaurantReport> byRestaurant = {};
        foreach OrderRecord o in orders {
            if !byRestaurant.hasKey(o.restaurantId) {
                byRestaurant[o.restaurantId] = {
                    restaurantId: o.restaurantId,
                    totalOrders: 0,
                    delivered: 0,
                    cancelled: 0,
                    revenue: 0.0
                };
            }
            RestaurantReport report = byRestaurant.get(o.restaurantId);
            report.totalOrders += 1;
            if o.status == "DELIVERED" {
                report.delivered += 1;
            }
            if o.status == "CANCELLED" {
                report.cancelled += 1;
            } else if o.status != "CREATED" {
                report.revenue += o.totalAmount;    // paid orders count as revenue
            }
        }
        return byRestaurant.toArray();
    }

    // GET /reports/deliveries  -> delivery performance
    resource function get deliveries() returns DeliveryReport|error {
        stream<DeliveryRecord, error?> results = check deliveriesCollection->find();
        DeliveryRecord[] deliveries = check from DeliveryRecord d in results select d;

        int completed = 0;
        float totalMinutes = 0.0;
        map<DriverReport> byDriver = {};
        map<float> driverMinutes = {};

        foreach DeliveryRecord d in deliveries {
            if !byDriver.hasKey(d.driverId) {
                byDriver[d.driverId] = {driverId: d.driverId, completedDeliveries: 0, averageMinutes: 0.0};
                driverMinutes[d.driverId] = 0.0;
            }
            if d.assignedAt == "" || d.deliveredAt == "" {
                continue;   // still on the road
            }
            float minutes = check minutesBetween(d.assignedAt, d.deliveredAt);
            completed += 1;
            totalMinutes += minutes;

            DriverReport driver = byDriver.get(d.driverId);
            driver.completedDeliveries += 1;
            driverMinutes[d.driverId] = driverMinutes.get(d.driverId) + minutes;
        }

        foreach DriverReport driver in byDriver {
            if driver.completedDeliveries > 0 {
                driver.averageMinutes = driverMinutes.get(driver.driverId) / <float>driver.completedDeliveries;
            }
        }

        return {
            totalDeliveries: deliveries.length(),
            completed: completed,
            inProgress: deliveries.length() - completed,
            averageMinutes: completed > 0 ? totalMinutes / <float>completed : 0.0,
            drivers: byDriver.toArray()
        };
    }
}