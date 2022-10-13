package com.audit.trail.datagenerator

import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories

@SpringBootApplication
@EnableMongoRepositories
class DataGeneratorApplication

fun main(args: Array<String>) {
	runApplication<DataGeneratorApplication>(*args)
}
