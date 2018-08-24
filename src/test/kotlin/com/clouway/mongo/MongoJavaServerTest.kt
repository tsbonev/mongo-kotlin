package com.clouway.mongo

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat


/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class MongoJavaServerTest {

    lateinit var collection: MongoCollection<Document>
    lateinit var client: MongoClient
    lateinit var server: MongoServer

    @Before
    fun setUp(){
        server = MongoServer(MemoryBackend())
        val serverAddress = server.bind()
        client = MongoClient(ServerAddress(serverAddress))
        collection = client.getDatabase("testdb").getCollection("testcollection")
    }

    @After
    fun tearDown(){
        client.close()
        server.shutdown()
    }

    @Test
    fun insertDocument(){
        assertThat(collection.count(), Is(0L))

        val obj = Document("_id", 1).append("key", "value")
        collection.insertOne(obj)

        assertThat(collection.count(), Is(1L))
        assertThat(collection.find().first(), Is(obj))
    }
}