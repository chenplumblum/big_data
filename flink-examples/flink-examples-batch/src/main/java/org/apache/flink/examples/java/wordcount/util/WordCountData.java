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

package org.apache.flink.examples.java.wordcount.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Provides the default data sets used for the WordCount example program.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class WordCountData {

	public static final String[] WORDS = new String[] { "苹果", "梨", "西瓜", "葡萄", "火龙果", "梨", "梨","苹果"};

	public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
		return env.fromElements(WORDS);
	}
}