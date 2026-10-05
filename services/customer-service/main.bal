import ballerina/http;
import ballerina/time;
import ballerina/uuid;
import ballerinax/mongodb;

configurable int port = 8081;
configurable string kafkaBootstrap = "localhost:29092";
configurable string mongoUrl = "mongodb://localhost:27017";

final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection customersCollection = check getCollection("customers");
final mongodb:Collection historyCollection = check getCollection("order_history");

function getCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("customer_db");
    return db->getCollection(name);
}

function nowString() returns string => time:utcToString(time:utcNow());

function newId(string prefix) returns string =>
    prefix + uuid:createType4AsString().substring(0, 8).toUpperAscii();

service /customers on new http:Listener(port) {

    // POST /customers  -> sign up
    resource function post .(@http:Payload NewCustomerRequest req)
            returns http:Created|http:BadRequest|error {
        if !req.email.includes("@") {
            return <http:BadRequest>{body: {message: "Please give a valid email address"}};
        }
        Customer customer = {
            customerId: newId("CUST-"),
            name: req.name,
            email: req.email,
            phone: req.phone,
            addresses: [],
            createdAt: nowString()
        };
        check customersCollection->insertOne(customer);
        return <http:Created>{body: customer};
    }

    // GET /customers
    resource function get .() returns Customer[]|error {
        stream<Customer, error?> results = check customersCollection->find();
        return from Customer c in results select c;
    }

    // GET /customers/CUST-1234
    resource function get [string customerId]() returns Customer|http:NotFound|error {
        Customer? customer = check customersCollection->findOne({customerId: customerId});
        if customer is () {
            return <http:NotFound>{body: {message: string `Customer ${customerId} not found`}};
        }
        return customer;
    }

    // POST /customers/CUST-1234/addresses  -> add a delivery address
    resource function post [string customerId]/addresses(@http:Payload NewAddressRequest req)
            returns http:Created|http:NotFound|error {
        Customer? customer = check customersCollection->findOne({customerId: customerId});
        if customer is () {
            return <http:NotFound>{body: {message: string `Customer ${customerId} not found`}};
        }
        Address address = {
            addressId: newId("ADDR-"),
            label: req.label,
            street: req.street,
            city: req.city
        };
        customer.addresses.push(address);
        _ = check customersCollection->updateOne({customerId: customerId}, {
            set: {addresses: customer.addresses.toJson()}
        });
        return <http:Created>{body: address};
    }

    // GET /customers/CUST-1234/addresses
    resource function get [string customerId]/addresses() returns Address[]|http:NotFound|error {
        Customer? customer = check customersCollection->findOne({customerId: customerId});
        if customer is () {
            return <http:NotFound>{body: {message: string `Customer ${customerId} not found`}};
        }
        return customer.addresses;
    }

    // GET /customers/CUST-1234/orders  -> order history
    resource function get [string customerId]/orders() returns OrderHistoryEntry[]|error {
        stream<OrderHistoryEntry, error?> results = check historyCollection->find({customerId: customerId});
        return from OrderHistoryEntry h in results select h;
    }
}