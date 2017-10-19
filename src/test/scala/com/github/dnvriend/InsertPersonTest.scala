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

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.Scanamo._

import scalaz._
import scalaz.Scalaz._

case class Person(name: String, age: Int)

case class Counter(name: String, value: Int)

class InsertPersonTest extends TestSpec {
  it should "insert a person" in withTable("personTable", 'name -> S) { client => tableName =>
    import com.gu.scanamo.syntax._
    Scanamo.put(client)(tableName)(Person("dennis", 42))
    Scanamo.get[Person](client)(tableName)('name -> "dennis").value.disjunction should beRight(Person("dennis", 42))
  }

  it should "update a person" in withTable("personTable", 'name -> S) { client => tableName =>
    import com.gu.scanamo.syntax._
    Scanamo.put(client)(tableName)(Person("dennis", 42))
    Scanamo.update[Person](client)(tableName)('name -> "dennis", set('age -> 43)).disjunction should beRight(Person("dennis", 43))
  }

  it should "update a counter" in withTable("counterTable", 'name -> S) { client => tableName =>
    import com.gu.scanamo.syntax._
    for {
      Counter("c1", 1) <- Scanamo.update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
      Counter("c1", 2) <- Scanamo.update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
      Counter("c1", 3) <- Scanamo.update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
      Counter("c1", 4) <- Scanamo.update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
      result <- Scanamo.get[Counter](client)(tableName)('name -> "c1")
    } yield result shouldBe Right(Counter("c1", 4))
  }

  it should "create a person" in withDynamoAndTable("personTable", 'name -> S) { dynamo =>
    import com.gu.scanamo.syntax._
    dynamo.put[Person](Person("foobar", 20))
    dynamo.get[Person]('name -> "foobar") should beRight(Person("foobar", 20))
  }
}
