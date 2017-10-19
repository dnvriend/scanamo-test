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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{ BatchWriteItemResult, DeleteItemResult, PutItemResult }
import com.gu.scanamo.{ DynamoFormat, Scanamo, ScanamoFree }
import com.gu.scanamo.error.{ DynamoReadError, MissingProperty, TypeCoercionError }
import com.gu.scanamo.ops.{ ScanamoInterpreters, ScanamoOps }
import com.gu.scanamo.query.{ Query, UniqueKey, UniqueKeys }
import com.gu.scanamo.update.UpdateExpression

import scalaz._
import scalaz.Scalaz._

class Dynamo(client: AmazonDynamoDB, tableName: String) {

  def exec[A](op: ScanamoOps[A]): A = Scanamo.exec(client)(op)

  def put[T: DynamoFormat](item: T): Disjunction[Throwable, PutItemResult] = {
    Disjunction.fromTryCatchNonFatal(exec(ScanamoFree.put(tableName)(item)))
  }

  def putAll[T: DynamoFormat](items: Set[T]): Disjunction[Throwable, List[BatchWriteItemResult]] =
    Disjunction.fromTryCatchNonFatal(exec(ScanamoFree.putAll(tableName)(items)))

  def get[T: DynamoFormat](key: UniqueKey[_]): Disjunction[DynamoReadError, T] = {
    exec(ScanamoFree.get[T](tableName)(key))
      .toRightDisjunction(MissingProperty)
      .flatMap(_.disjunction)
  }

  def getWithConsistency[T: DynamoFormat](key: UniqueKey[_]): Disjunction[DynamoReadError, T] = {
    exec(ScanamoFree.getWithConsistency[T](tableName)(key))
      .toRightDisjunction(MissingProperty)
      .flatMap(_.disjunction)
  }

  def getAll[T: DynamoFormat](keys: UniqueKeys[_]): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.getAll(tableName)(keys)).toList.sequenceU.disjunction
  }

  def delete(key: UniqueKey[_]): Disjunction[DynamoReadError, DeleteItemResult] = {
    Disjunction.fromTryCatchNonFatal(exec(ScanamoFree.delete(tableName)(key)))
      .leftMap(t => TypeCoercionError(t))
  }

  def deleteAll(client: AmazonDynamoDB)(tableName: String)(items: UniqueKeys[_]): Disjunction[DynamoReadError, List[BatchWriteItemResult]] = {
    Disjunction.fromTryCatchNonFatal(exec(ScanamoFree.deleteAll(tableName)(items)))
      .leftMap(t => TypeCoercionError(t))
  }

  def update[V: DynamoFormat](key: UniqueKey[_], expression: UpdateExpression): Disjunction[DynamoReadError, V] = {
    Disjunction.fromTryCatchNonFatal(exec(ScanamoFree.update[V](tableName)(key)(expression)))
      .leftMap(t => TypeCoercionError(t))
      .flatMap(_.disjunction)
  }

  def scan[T: DynamoFormat]: Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.scan(tableName)).sequenceU.disjunction
  }

  def scanWithLimit[T: DynamoFormat](limit: Int): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.scanWithLimit(tableName, limit)).map(_.disjunction).sequenceU
  }

  def scanIndex[T: DynamoFormat](indexName: String): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.scanIndex(tableName, indexName)).sequenceU.disjunction
  }

  def scanIndexWithLimit[T: DynamoFormat](indexName: String, limit: Int): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.scanIndexWithLimit(tableName, indexName, limit)).sequenceU.disjunction
  }

  def query[T: DynamoFormat](query: Query[_]): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.query(tableName)(query)).sequenceU.disjunction
  }

  def queryWithLimit[T: DynamoFormat](query: Query[_], limit: Int): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.queryWithLimit(tableName)(query, limit)).sequenceU.disjunction
  }

  def queryIndex[T: DynamoFormat](indexName: String)(query: Query[_]): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.queryIndex(tableName, indexName)(query)).sequenceU.disjunction
  }

  def queryIndexWithLimit[T: DynamoFormat](indexName: String)(query: Query[_], limit: Int): Disjunction[DynamoReadError, List[T]] = {
    exec(ScanamoFree.queryIndexWithLimit(tableName, indexName)(query, limit)).sequenceU.disjunction
  }
}
