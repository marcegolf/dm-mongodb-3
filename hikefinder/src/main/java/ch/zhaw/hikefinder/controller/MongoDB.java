package ch.zhaw.hikefinder.controller;

import java.util.ArrayList;
import java.util.Arrays;
import org.bson.Document;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.github.cdimascio.dotenv.Dotenv;

@RestController
public class MongoDB {
    private static MongoDatabase myDB = null;
    private static String databaseName = "general";

    private static Dotenv dotenv = Dotenv.load();
    private static String connectionString = dotenv.get("DB_URI");

    public MongoDB() {
        try {
            ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();

            MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();

            MongoClient mongoClient = MongoClients.create(settings);
            myDB = mongoClient.getDatabase(databaseName);

            System.out.println("AAAAAA");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/testmongodb")
    public String index() {
        ListCollectionNamesIterable col = myDB.listCollectionNames();
        ArrayList<String> colList = col.into(new ArrayList<String>());

        return colList.toString();
    }

    @GetMapping("/api/hikes")
    public ResponseEntity<Object> getHikes(@RequestParam(defaultValue = "0") int distance,
            @RequestParam(required = false) String canton) {

        try {
            MongoCollection<Document> hikeCol = myDB.getCollection("hikes");
            AggregateIterable<Document> result = null;

            if (distance == 0) {
                result = hikeCol.aggregate(Arrays.asList(new Document("$sample", new Document("size", 3L))));
            } else if (canton != null && !canton.isEmpty()) {
                result = hikeCol.aggregate(Arrays.asList(new Document("$lookup",
                        new Document("from", "locations")
                                .append("localField", "from_name")
                                .append("foreignField", "ortschaft")
                                .append("as", "kanton")),
                        new Document("$project",
                            new Document("from_name", 1L)
                                    .append("to_name", 1L)
                                    .append("year", 1L)
                                    .append("month", 1L)
                                    .append("distance", 1L)
                                    .append("altitude_diff", 1L)
                                    .append("kanton", "$kanton.kanton")),
                        new Document("$match",
                                new Document("distance",
                                new Document("$gt", distance - 2)
                                .append("$lt", distance + 2))
                            .append("kanton", canton))));
            } else {
                result = hikeCol.aggregate(Arrays.asList(new Document("$match",
                        new Document("distance",
                        new Document("$gt", distance - 2).append("$lt", distance + 2))),
                        new Document("$sample",
                        new Document("size", 3L))));
            }

            ArrayList<Document> hikes = result.into(new ArrayList<Document>());
            return ResponseEntity.ok(hikes);
        } catch (Exception e) {
            return ResponseEntity.status(500).build();
        }
    }
}