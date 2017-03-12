/**
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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator.BufferType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFCollectSet
 */
@Description(name = "collect_set", value = "_FUNC_(x, y) - Returns a set of objects with duplicate elements eliminated")
public class GenericUDAFCollectSet extends AbstractGenericUDAFResolver {

  public GenericUDAFCollectSet() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length < 1 || parameters.length > 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Expecting 1 or 2 parameters");
    }
    switch (parameters[0].getCategory()) {
      case PRIMITIVE:
      case STRUCT:
      case MAP:
      case LIST:
        break;
      default:
        throw new UDFArgumentTypeException(0,
            "Only primitive, struct, list or map type arguments are accepted but "
                + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    if(parameters.length == 2 && !parameters[1].getTypeName().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      throw new UDFArgumentTypeException(1,
              "Only boolean type argument is accepted but "
                      + parameters[1].getTypeName() + " was passed as parameter 2.");
    }

    return new GenericUDAFMkCollectionEvaluator(BufferType.SET);
  }

}
