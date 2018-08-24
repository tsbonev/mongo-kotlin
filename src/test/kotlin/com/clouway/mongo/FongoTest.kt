package com.clouway.mongo

import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import java.util.*
import com.github.fakemongo.junit.FongoRule
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.Updates.*
import org.bson.BsonType
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.time.Instant
import java.util.concurrent.TimeUnit
import com.mongodb.client.model.CreateCollectionOptions




/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class FongoTest {

    @Rule
    @JvmField
    val fongoRule = FongoRule()

    private val john = Person(
            UUID.randomUUID(),
            "John",
            26,
            mapOf("torso" to "shirt",
                    "feet" to "shoes")
    )
    private val johnDoc = Document(mapOf("_id" to john.id,
            "name" to john.name,
            "age" to john.age,
            "clothes" to john.clothes))

    private val peter = Person(
            UUID.randomUUID(),
            "Peter",
            31,
            mapOf("feet" to "sandals")
    )
    private val peterDoc = Document(mapOf("_id" to peter.id,
            "name" to peter.name,
            "age" to peter.age,
            "clothes" to peter.clothes))

    private val ann = Person(
            UUID.randomUUID(),
            "Ann",
            23,
            mapOf("torso" to "dress",
                    "feet" to "heels",
                    "head" to "hat")
    )
    private val annDoc = Document(mapOf("_id" to ann.id,
            "name" to ann.name,
            "age" to ann.age,
            "clothes" to ann.clothes))

    lateinit var db: MongoDatabase
    lateinit var coll: MongoCollection<Document>

    @Before
    fun setUp(){
        db = fongoRule.mongoClient.getDatabase("mydb")
        db.getCollection("people").createIndex(Indexes.ascending("name"))
        // _id is indexed by default
        db.getCollection("people").insertMany(
                listOf(johnDoc, peterDoc, annDoc)
        )
        coll = db.getCollection("people")
    }

    @After
    fun cleanUp(){
        db.drop()
    }

    @Test
    fun writeDocument(){
        coll.insertOne(Document("_id", 123))

        val cursor = db.getCollection("people").find(eq("_id", 123))

        assertThat(cursor.first().getInteger("_id"), Is(123))
    }

    @Test
    fun createCappedCollection(){
        db.createCollection("log", CreateCollectionOptions()
                .capped(true)
                .maxDocuments(100)
                .autoIndex(false) // don't index "_id" field
                .sizeInBytes(256))
        // Mongo's minimum cap is 256
        // Size always round up to the next multiple of 256
    }

    @Test
    fun writeTemporaryDocument(){
        coll.createIndex(Indexes.ascending("expirationDate"),
                IndexOptions().expireAfter(1, TimeUnit.DAYS))
        // TTL indexes must be date fields
        // Fongo as of 18.08.24 does not support TTL indexes

        // MongoDB itself runs a background thread every 60 seconds
        // checking for expired TTL

        // Setting expireAfter to 0
        // makes the set date against the field
        // the de facto expiration date

        coll.insertOne(Document(mapOf(
                "_id" to 123,
                "expirationDate" to Date.from(Instant.now())
        )))

        val cursor = db.getCollection("people").find(eq("_id", 123))

        assertThat(cursor.first().getInteger("_id"), Is(123))
    }

    @Test
    fun readDocument(){
        val cursor = coll.find(eq("name", "John"))

        assertThat(cursor.first().getInteger("age"), Is(john.age))
    }

    @Test
    fun updateDocumentField(){
        coll.updateOne(eq("name", "John"), set("name", "Johny"))

        val cursor = coll.find(eq("_id", john.id))

        assertThat(cursor.first().getString("name"), Is("Johny"))
    }

    @Test
    fun combinedUpdate(){
        coll.updateOne(eq("name", "John"), combine(set("name", "Johny"), set("age", 19)))

        val cursor = coll.find(eq("_id", john.id))

        assertThat(cursor.first().getString("name"), Is("Johny"))
        assertThat(cursor.first().getInteger("age"), Is(19))
    }

    @Test
    fun deleteDocument(){
        val collCount = coll.count()
        coll.deleteOne(eq("name", john.name))
        assertThat(coll.count(), Is(collCount - 1))
    }

    @Test
    fun deleteDocumentGroupByType(){
        coll.deleteMany(type("name", BsonType.STRING))
        assertThat(coll.count(), Is(0L))
    }

    @Test
    fun queryByAnd(){
        val cursor = coll.find(and(eq("name", "John"), eq("age", john.age)))
        assertThat(cursor.count(), Is(1))
    }

    @Test
    fun queryByOr(){
        val cursor = coll.find(or(eq("name", "John"), eq("age", ann.age)))
        assertThat(cursor.count(), Is(2))
    }

    @Test
    fun queryComposite(){
        val cursor = coll.find(
                or(
                    eq("name", "John"),
                    and(
                        eq("age", ann.age),
                        eq("clothes", mapOf("feet" to "sandals"))
                    )
                )
        )
        assertThat(cursor.count(), Is(1))
    }

    @Test
    fun queryInverse(){
        val cursor = coll.find(not(eq("name", "John")))

        assertThat(cursor.count(), Is(2))
    }

    @Test
    fun queryNor(){
        val cursor = coll.find(
                nor(
                        eq("name", "John"),
                        eq("age", ann.age)
                )
        )
        assertThat(cursor.count(), Is(1))
    }

    @Test
    fun queryRegex(){
        val cursor = coll.find(
                regex("name", "^[J]+.*$")
        )
        assertThat(cursor.count(), Is(1))
    }

    @Test
    fun queryOnSubdocumentCollection(){
        val cursor = coll.find(
                eq("clothes.feet", "sandals")
        )
        assertThat(cursor.first().getString("name"), Is(peter.name))
    }

    @Test
    fun queryNumeric(){
        val cursor = coll.find(
                gte("age", 23)
        )
        assertThat(cursor.count(), Is(3))
    }

    @Test
    fun atomicQuery(){
        val cursor = coll.findOneAndUpdate(
                eq("name", "John"), set("name", "Steve")
        )
        assertThat(cursor.getString("name"), Is("John"))
        //returns object before update
        val updatedCursor = coll.find(eq("name", "Steve"))
        //object is updated
        assertThat(updatedCursor.first().getString("name"), Is("Steve"))
        assertThat(updatedCursor.first().getInteger("age"), Is(john.age))

    }

    @Test
    fun queryPagination(){
        val cursor = coll.find(
                gte("age", 23)
        ).skip(1).limit(1)
        assertThat(cursor.count(), Is(1))
    }

    @Test
    fun sortQuery(){
        val ascending = coll.find(
                gte("age", 23)
        ).sort(Document("name", 1))
        val descending = coll.find(
                gte("age", 23)
        ).sort(Document("name", -1))
        // The name field is indexes so this sort will be
        // faster than others
        assertThat(ascending.first().getString("name"), Is(ann.name))
        assertThat(descending.first().getString("name"), Is(peter.name))
    }
    
    @Test
    fun compositeSort(){
        val duplicateAnn = Document(mapOf("_id" to UUID.randomUUID(),
                "name" to ann.name,
                "age" to john.age,
                "clothes" to john.clothes))

        coll.insertOne(duplicateAnn)
        val ascNameDescAge = coll.find(
                gte("age", 23)
        ).sort(and(Document("name", 1), Document("age", -1)))
        assertThat(ascNameDescAge.first().getInteger("age"), Is(john.age))
        assertThat(ascNameDescAge.skip(1).first().getString("name"), Is(ann.name))
    }

    @Test
    fun bulkWrite(){
        coll.bulkWrite(
                listOf(
                        InsertOneModel(Document("name", "Ivan")),
                        DeleteOneModel(Document("name", "John")),
                        UpdateOneModel(Document("name", "Ivan"), set("age", 19))
                ),
                BulkWriteOptions().ordered(true)
        //Ordered is true by default
        )
        val cursor = coll.find(eq("name", "Ivan"))
        assertThat(cursor.first().getInteger("age"), Is(19))
    }

    @Test
    fun performCountAggregation(){

        val duplicateAnn = Document(mapOf("_id" to UUID.randomUUID(),
                "name" to ann.name,
                "age" to john.age,
                "clothes" to john.clothes))

        coll.insertOne(duplicateAnn)

        val cursor = coll.aggregate(
                listOf(
                        Aggregates.match(Filters.gte("age", 23)),
                        Aggregates.group("name",
                                Accumulators.sum("count", 1)),
                        Aggregates.sort(Document("_id", 1))
                )
        )

        assertThat(cursor.first().getInteger("count"), Is(2))
    }
}