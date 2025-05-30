/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wayang.api.json.operatorfromjson.unary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.util.NDimArray

@JsonTypeName(OperatorFromJson.OperatorNames.Sort)
case class SortOperatorFromJson(override val id: Long,
                                override val input: Array[Long],
                                override val output: Array[Long],
                                override val cat: String,
                                override val operatorName: String,
                                val data: SortOperatorFromJson.Data,
                                override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

object SortOperatorFromJson {
  case class Data(keyUdf: String, val inputType: scala.Option[NDimArray], val outputType: scala.Option[NDimArray])
}
