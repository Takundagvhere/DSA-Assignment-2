import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;

configurable int port = 8086;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";

final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection notificationsCollection = check getCollection();

function getCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("notification_db");
    return db->getCollection("notifications");
}

function nowString() returns string => time:utcToString(time:utcNow());

// ===================================================
// Sends one message on several channels (simulated).
// The ID is built from order + event + person + channel,
// so a duplicate Kafka event never sends the same SMS twice.
// ===================================================
function notify(string orderId, string eventName, string recipientType,
        string recipientId, string message, string[] channels) returns error? {
    foreach string channel in channels {
        string notificationId = string `${orderId}-${eventName}-${recipientType}-${channel}`;
        Notification? existing = check notificationsCollection->findOne({notificationId: notificationId});
        if existing is Notification {
            continue;   // already sent
        }
        Notification n = {
            notificationId: notificationId,
            orderId: orderId,
            recipientType: recipientType,
            recipientId: recipientId,
            channel: channel,
            message: message,
            sentAt: nowString()
        };
        check notificationsCollection->insertOne(n);
        log:printInfo(string `[${channel}] to ${recipientType} ${recipientId}: ${message}`);
    }
}

// ===================================================
// REST API - see what was sent
// GET /notifications  (optional ?recipientId=...  ?orderId=...)
// ===================================================
service /notifications on new http:Listener(port) {
    resource function get .(string? recipientId, string? orderId) returns Notification[]|error {
        map<json> filter = {};
        if recipientId is string {
            filter["recipientId"] = recipientId;
        }
        if orderId is string {
            filter["orderId"] = orderId;
        }
        stream<Notification, error?> results = check notificationsCollection->find(filter);
        return from Notification n in results select n;
    }
}