/*
 * Copyright 2017 Dennis Vriend
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dnvriend

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._

object LocalDynamoDB {
  def asyncClient(): AmazonDynamoDBAsync =
    AmazonDynamoDBAsyncClient.asyncBuilder()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "credentials")))
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8042", ""))
      .build()

  def client(): AmazonDynamoDB =
    AmazonDynamoDBClient.builder()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "credentials")))
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8042", ""))
      .build()

  def createTable(client: AmazonDynamoDB)(tableName: String)(attributes: (Symbol, ScalarAttributeType)*): CreateTableResult = {
    client.createTable(
      attributeDefinitions(attributes),
      tableName,
      keySchema(attributes),
      arbitraryThroughputThatIsIgnoredByDynamoDBLocal
    )
  }

  def withTable[T](client: AmazonDynamoDB)(tableName: String)(attributeDefinitions: (Symbol, ScalarAttributeType)*)(
    thunk: => T
  ): T = {
    createTable(client)(tableName)(attributeDefinitions: _*)
    val res = try {
      thunk
    } finally {
      client.deleteTable(tableName)
      ()
    }
    res
  }

  def usingTable[T](client: AmazonDynamoDB)(tableName: String)(attributeDefinitions: (Symbol, ScalarAttributeType)*)(
    thunk: => T
  ): Unit = {
    withTable(client)(tableName)(attributeDefinitions: _*)(thunk)
    ()
  }

  def withTableWithSecondaryIndex[T](client: AmazonDynamoDB)(tableName: String, secondaryIndexName: String)(primaryIndexAttributes: (Symbol, ScalarAttributeType)*)(secondaryIndexAttributes: (Symbol, ScalarAttributeType)*)(
    thunk: => T
  ): T = {
    client.createTable(
      new CreateTableRequest().withTableName(tableName)
        .withAttributeDefinitions(attributeDefinitions(
          primaryIndexAttributes.toList ++ (secondaryIndexAttributes.toList diff primaryIndexAttributes.toList)
        ))
        .withKeySchema(keySchema(primaryIndexAttributes))
        .withProvisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
        .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
          .withIndexName(secondaryIndexName)
          .withKeySchema(keySchema(secondaryIndexAttributes))
          .withProvisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
          .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
    )
    val res = try {
      thunk
    } finally {
      client.deleteTable(tableName)
      ()
    }
    res
  }

  private def keySchema(attributes: Seq[(Symbol, ScalarAttributeType)]) = {
    val hashKeyWithType :: rangeKeyWithType = attributes.toList
    val keySchemas = hashKeyWithType._1 -> KeyType.HASH :: rangeKeyWithType.map(_._1 -> KeyType.RANGE)
    keySchemas.map { case (symbol, keyType) => new KeySchemaElement(symbol.name, keyType) }.asJava
  }

  private def attributeDefinitions(attributes: Seq[(Symbol, ScalarAttributeType)]) = {
    attributes.map { case (symbol, attributeType) => new AttributeDefinition(symbol.name, attributeType) }.asJava
  }

  private val arbitraryThroughputThatIsIgnoredByDynamoDBLocal = new ProvisionedThroughput(1L, 1L)
}
