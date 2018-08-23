package com.clouway.mongo

import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import java.util.*
import com.github.fakemongo.junit.FongoRule
import com.mongodb.client.MongoDatabase
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


    private val objId = UUID.randomUUID()!!

    val name = "John"
    val age = 26
    val clothes = mapOf("torso" to "shirt",
            "feet" to "shoes")

    lateinit var db: MongoDatabase

    @Before
    fun setUp(){
        db = fongoRule.fongo.getDatabase("mydb")
        db
    }

    @After
    fun cleanUp(){
        db.drop()
    }

    @Test
    fun writeToDatabase(){
    }
}