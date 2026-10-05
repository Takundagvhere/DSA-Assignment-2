import ballerina/log;
import ballerinax/kafka;

listener kafka:Listener orderStatusListener = new (kafkaBootstrap, {
    groupId: "delivery-service",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on orderStatusListener {
    remote function onConsumerRecord(OrderStatusChangedEvent[] events) {
        foreach OrderStatusChangedEvent e in events {
            if e.newStatus != "READY" {
                continue;   // we only care about food that's ready
            }
            error? result = createDelivery(e);
            if result is error {
                log:printError(string `Could not create delivery for ${e.orderId}`, result);
            }
        }
    }
}