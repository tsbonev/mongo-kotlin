package com.clouway.mongo

import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import java.util.*
import com.github.fakemongo.junit.FongoRule
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test


/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class MongoDbTest {

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

    @Before
    fun setUp(){
        db = fongoRule.fongo.getDatabase("mydb")
        db.getCollection("people").insertMany(
                listOf(johnDoc, peterDoc, annDoc)
        )
    }

    @After
    fun cleanUp(){
        db.drop()
    }

    @Test
    fun writeToDatabase(){

        val coll = db.getCollection("people")
        coll.insertOne(Document("_id", 123))

        val cursor = db.getCollection("people").find(eq("_id", 123))

        assertThat(cursor.first().getInteger("_id"), Is(123))
    }

    @Test
    fun readFromDatabase(){
        val coll = db.getCollection("people")
        val cursor = coll.find(eq("name", "John"))

        assertThat(cursor.first().getInteger("age"), Is(john.age))
    }

    @Test
    fun updateInDatabase(){
        val coll = db.getCollection("people")
        coll.updateOne(eq("name", "John"), Document(
                "\$set", Document("name", "Johny")
        ))

        val cursor = coll.find(eq("_id", john.id))

        assertThat(cursor.first().getString("name"), Is("Johny"))
    }

    @Test
    fun deleteFromDatabase(){
        val coll = db.getCollection("people")
        val collCount = coll.count()
        coll.deleteOne(eq("name", john.name))
        assertThat(coll.count(), Is(collCount - 1))
    }
}