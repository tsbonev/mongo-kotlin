package com.clouway.mongo

import com.github.fakemongo.junit.FongoRule
import org.bson.Document
import org.junit.Rule
import org.junit.Test
import java.util.*

/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class TransactionTest {

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

    private val johnyDoc = Document(mapOf("_id" to UUID.randomUUID(),
            "name" to "Johny",
            "age" to john.age,
            "clothes" to john.clothes))

    @Test
    fun doTransaction(){

        val client = fongoRule.mongoClient

        val db = client.getDatabase("testdb")
        val collection = db.getCollection("test")

        //As of 18.08.23 mongo-driver 3.8.1 supports transactions
        //but fongo 2.2.0-RC2 does not support mongo-driver 3.8.1

        /*client.startSession().use { clientSession ->
            clientSession.startTransaction()
            collection.insertOne(clientSession, johnDoc)
            collection.insertOne(clientSession, johnyDoc)
            clientSession.commitTransaction()
        }*/

    }

}