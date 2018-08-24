package com.clouway.mongo

import com.github.fakemongo.junit.FongoRule
import com.mongodb.client.model.Filters.and
import com.mongodb.client.model.Filters.eq
import org.bson.Document
import org.junit.Rule
import org.junit.Test
import java.time.Instant
import java.time.LocalDateTime
import java.util.*
import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import kotlin.collections.ArrayList

/**
 * Inconvertible types:
 * Non-byte arrays
 * LocalDateTime
 * Lists with LocalDateTime
 * Instant
 * Custom objects
 *
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class DocumentConversionTest {

    @Rule
    @JvmField
    val fongoRule = FongoRule()

    data class TestData(val data: String)

    @Test
    fun documentConversion() {

        val id = UUID.randomUUID()
        val intVal = 1
        val longVal = 1L
        val doubleVal = 1.00
        val stringVal = "123"
        val intArrayVal = arrayOf(1, 2, 3)
        val charArrayVal = arrayOf('1', '2', '3')
        val javaArray = arrayOf(Integer(1), Integer(2), Integer(3))
        val byteArray = byteArrayOf(1, 2, 3)
        val homogeneousListVal = listOf("123", "231", "321")
        val heterogeneousListVal = listOf("123", 123, 123.123)
        val dateInListVal = listOf("123", 123, LocalDateTime.now())
        val mapVal = mapOf("a" to "b", "c" to "d")
        val ldtVal = LocalDateTime.now()
        val utilDateVal = Date.from(Instant.now())
        val instant = Instant.now()
        val objVal = TestData("::data::")

        val document = Document()

        document.append("_id", id)
        document.append("intVal", intVal)
        document.append("longVal", longVal)
        document.append("doubleVal", doubleVal)
        document.append("stringVal", stringVal)

//        document.append("intArrayVal", intArrayVal)
//        document.append("charArrayVal", charArrayVal)
//        document.append("javaArray", javaArray)
        //Some array types cannot find codecs

        document.append("byteArrayVal", byteArray)
        document.append("homListVal", homogeneousListVal)
        document.append("hetListVal", heterogeneousListVal)

//        document.append("dateListVal", dateInListVal)

        document.append("mapVal", mapVal)

//        document.append("ldtVal", ldtVal)
        //LocalDateTime has no codec

        document.append("utilDateVal", utilDateVal)

//        document.append("instantVal", instant)

//        document.append("objVal", objVal)
        //Custom objects have no codec

        val db = fongoRule.mongoClient.getDatabase("testDb")
        db.getCollection("testCollection").insertOne(document)

        val cursor = db.getCollection("testCollection").find(
                and(
                        eq("_id", id),
                        eq("intVal", intVal),
                        eq("longVal", longVal),
                        eq("doubleVal", doubleVal),
                        eq("stringVal", stringVal),
//                    eq("intArrayVal", intArrayVal),
//                    eq("charArrayVal", charArrayVal),
//                    eq("javaArray", javaArray),
                        eq("byteArrayVal", byteArray),
                        eq("homListVal", homogeneousListVal),
                        eq("hetListVal", heterogeneousListVal),
//                    eq("dateListVal", dateInListVal),
                        eq("mapVal", mapVal),
//                    eq("ldtVal", ldtVal),
                        eq("utilDateVal", utilDateVal)
//                    eq("instantVal", instant)
//                    eq("objVal", objVal)
                )
        )

        println(cursor.first())

        println(cursor.first().javaClass)

        assertThat(cursor.first()["homListVal"]!!.javaClass.name, Is(ArrayList::class.java.name))
        assertThat(cursor.first()["intVal"]!!.javaClass.name, Is(java.lang.Integer::class.java.name))

        assertThat(cursor.first()["homListVal"] as List<String>, Is(homogeneousListVal))
        assertThat(cursor.first()["hetListVal", List::class.java] as List<Any>, Is(heterogeneousListVal))

        assertThat(cursor.first() == document, Is(false))
        //assertThat(cursor.first()["mapVal"] as Map<String, String>, Is(mapVal))
        //Maps get converted to Documents

        val documentMap = Document()
        mapVal.forEach {
            documentMap.append(it.key, it.value)
        }

        assertThat(cursor.first()["mapVal", Document::class.java], Is(documentMap))

        assertThat(cursor.first()["thisWontBeFound", "default"], Is("default"))
    }

}