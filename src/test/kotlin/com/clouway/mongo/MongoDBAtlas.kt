package com.clouway.mongo

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.model.Filters.eq
import org.bson.Document
import org.junit.Test


/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class MongoDBAtlas {

    var uri = MongoClientURI(
            "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/test?retryWrites=true")

    var mongoClient = MongoClient(uri)
    var database = mongoClient.getDatabase("test")
    var collection = database.getCollection("test")

    @Test
    fun shouldConnectToAtlas(){
        collection.insertOne(Document("_id", 123))
        collection.find(eq("_id", 123))
        collection.deleteOne(eq("_id", 123))
    }

}