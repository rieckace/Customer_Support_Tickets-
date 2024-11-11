package com.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
    // Database configuration
    private static final String MONGODB_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "Customer_Support_Tickets";
    private static final String COLLECTION_NAME = "customerTicket";

    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static MongoCollection<Document> collection;

    public static void main(String[] args) {
        try {
            // Initialize MongoDB connection with connection pooling
            initializeMongoConnection();

            // Example operations
            // 1. Create a new ticket
            createTicket(new Document()
                .append("TicketID", "10003")
                .append("CustomerID", "78901")
                .append("Description", "Network connectivity issues")
                .append("Priority", "High")
                .append("Status", "Open")
                .append("CreationDate", "2024-11-11T09:00:00Z")
                .append("LastUpdated", "2024-11-11T09:00:00Z")
                .append("AssignedTo", "tech_support_1")
                .append("Resolution", ""));

            // 2. Read all tickets
            System.out.println("\nAll Tickets:");
            getAllTickets().forEach(ticket -> System.out.println(ticket.toJson()));

            // 3. Update ticket status
            String ticketToUpdate = "10001";
            if (updateTicketStatus(ticketToUpdate, "In Progress")) {
                System.out.println("\nTicket " + ticketToUpdate + " status updated successfully!");
            }

            // 4. Get ticket by ID
            System.out.println("\nFetching specific ticket:");
            Document specificTicket = getTicketById(ticketToUpdate);
            if (specificTicket != null) {
                System.out.println(specificTicket.toJson());
            }

            // 5. Get tickets by status
            System.out.println("\nOpen Tickets:");
            getTicketsByStatus("Open").forEach(ticket -> System.out.println(ticket.toJson()));

            // 6. Get tickets by priority
            System.out.println("\nHigh Priority Tickets:");
            getTicketsByPriority("High").forEach(ticket -> System.out.println(ticket.toJson()));

            // 7. Update ticket resolution
            if (updateTicketResolution(ticketToUpdate, "Issue resolved - account access restored")) {
                System.out.println("\nTicket " + ticketToUpdate + " resolution updated and closed!");
            }

            // 8. Delete a ticket (commented out for safety)
            /*
            String ticketToDelete = "10003";
            if (deleteTicket(ticketToDelete)) {
                System.out.println("\nTicket " + ticketToDelete + " deleted successfully!");
            }
            */

        } catch (Exception e) {
            System.err.println("Error in database operations: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the MongoDB connection
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    // Initialize MongoDB connection with connection pooling
    private static void initializeMongoConnection() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .applyToConnectionPoolSettings(builder -> 
                    builder.maxSize(50)
                          .minSize(10)
                          .maxWaitTime(5000, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(builder ->
                    builder.connectTimeout(5000, TimeUnit.MILLISECONDS)
                          .readTimeout(5000, TimeUnit.MILLISECONDS))
                .build();

        mongoClient = MongoClients.create(settings);
        database = mongoClient.getDatabase(DATABASE_NAME);
        collection = database.getCollection(COLLECTION_NAME);
    }

    // Create operation
    private static void createTicket(Document ticket) {
        try {
            collection.insertOne(ticket);
            System.out.println("Ticket created successfully!");
        } catch (Exception e) {
            System.err.println("Error creating ticket: " + e.getMessage());
        }
    }

    // Read operations
    private static List<Document> getAllTickets() {
        List<Document> tickets = new ArrayList<>();
        try {
            FindIterable<Document> results = collection.find();
            for (Document doc : results) {
                tickets.add(doc);
            }
        } catch (Exception e) {
            System.err.println("Error retrieving tickets: " + e.getMessage());
        }
        return tickets;
    }

    private static Document getTicketById(String ticketId) {
        try {
            return collection.find(Filters.eq("TicketID", ticketId)).first();
        } catch (Exception e) {
            System.err.println("Error retrieving ticket: " + e.getMessage());
            return null;
        }
    }

    // Update operations
    private static boolean updateTicketStatus(String ticketId, String newStatus) {
        try {
            UpdateResult result = collection.updateOne(
                Filters.eq("TicketID", ticketId),
                Updates.set("Status", newStatus)
            );
            return result.getModifiedCount() > 0;
        } catch (Exception e) {
            System.err.println("Error updating ticket status: " + e.getMessage());
            return false;
        }
    }

    private static boolean updateTicketResolution(String ticketId, String resolution) {
        try {
            UpdateResult result = collection.updateOne(
                Filters.eq("TicketID", ticketId),
                Updates.combine(
                    Updates.set("Resolution", resolution),
                    Updates.set("Status", "Closed")
                )
            );
            return result.getModifiedCount() > 0;
        } catch (Exception e) {
            System.err.println("Error updating ticket resolution: " + e.getMessage());
            return false;
        }
    }

    // Delete operations
    private static boolean deleteTicket(String ticketId) {
        try {
            DeleteResult result = collection.deleteOne(Filters.eq("TicketID", ticketId));
            return result.getDeletedCount() > 0;
        } catch (Exception e) {
            System.err.println("Error deleting ticket: " + e.getMessage());
            return false;
        }
    }

    // Additional query methods
    private static List<Document> getTicketsByStatus(String status) {
        List<Document> tickets = new ArrayList<>();
        try {
            FindIterable<Document> results = collection.find(Filters.eq("Status", status));
            for (Document doc : results) {
                tickets.add(doc);
            }
        } catch (Exception e) {
            System.err.println("Error retrieving tickets by status: " + e.getMessage());
        }
        return tickets;
    }

    private static List<Document> getTicketsByPriority(String priority) {
        List<Document> tickets = new ArrayList<>();
        try {
            FindIterable<Document> results = collection.find(Filters.eq("Priority", priority));
            for (Document doc : results) {
                tickets.add(doc);
            }
        } catch (Exception e) {
            System.err.println("Error retrieving tickets by priority: " + e.getMessage());
        }
        return tickets;
    }
}