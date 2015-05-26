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

package org.apache.flink.types.parser;

import org.apache.flink.types.BooleanValue;

public class BooleanValueParser extends FieldParser<BooleanValue> {

	private BooleanParser parser = new BooleanParser();

	private BooleanValue result;

	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delim, BooleanValue reuse) {
		int returnValue = parser.parseField(bytes, startPos, limit, delim, reuse.getValue());
		setErrorState(parser.getErrorState());
		reuse.setValue(parser.getLastResult());
		result = reuse;
		return returnValue;
	}

	@Override
	public BooleanValue getLastResult() {
		return result;
	}

	@Override
	public BooleanValue createValue() {
		return new BooleanValue(false);
	}
}
