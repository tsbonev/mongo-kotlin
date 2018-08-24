package com.clouway.mongo

import java.time.LocalDateTime

/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
data class TestData(val data: String, val enum: TestEnum, val date: LocalDateTime)