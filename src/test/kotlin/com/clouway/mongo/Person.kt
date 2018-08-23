package com.clouway.mongo

import java.util.*

/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
data class Person (val id: UUID, val name: String, val age: Int, val clothes: Map<String, String>)