package com.clouway.mongo

import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import java.util.*
import com.github.fakemongo.junit.FongoRule
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.Updates.*
import org.bson.BsonType
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
    lateinit var coll: MongoCollection<Document>

    @Before
    fun setUp(){
        db = fongoRule.fongo.getDatabase("mydb")
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
    fun queryNumeric(){
        val cursor = coll.find(
                gte("age", 23)
        )
        assertThat(cursor.count(), Is(3))
    }
}