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
import com.gu.scanamo.Scanamo._

case class Person(name: String, age: Int)

case class Counter(name: String, value: Int)

class InsertPersonTest extends TestSpec {
  it should "insert a person" in withClient { client =>
    withTable("personTable", 'name -> S) { tableName =>
      import com.gu.scanamo.syntax._
      for {
        _ <- Option(put(client)(tableName)(Person("dennis", 42)))
        result <- get[Person](client)(tableName)('name -> "dennis")
      } yield result shouldBe Right(Person("dennis", 42))
    }
  }

  it should "update a person" in withClient { client =>
    withTable("personTable", 'name -> S) { tableName =>
      import com.gu.scanamo.syntax._
      for {
        _ <- Option(put(client)(tableName)(Person("dennis", 42))).toRight("")
        updated <- update[Person](client)(tableName)('name -> "dennis", set('age -> 43))
      } yield updated shouldBe Person("dennis", 43)
    }
  }

  it should "update a counter" in withClient { client =>
    withTable("counterTable", 'name -> S) { tableName =>
      import com.gu.scanamo.syntax._
      for {
        Counter("c1", 1) <- update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
        Counter("c1", 2) <- update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
        Counter("c1", 3) <- update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
        Counter("c1", 4) <- update[Counter](client)(tableName)('name -> "c1", add('value -> 1)).toOption
        result <- get[Counter](client)(tableName)('name -> "c1")
      } yield result shouldBe Right(Counter("c1", 4))
    }
  }
}
