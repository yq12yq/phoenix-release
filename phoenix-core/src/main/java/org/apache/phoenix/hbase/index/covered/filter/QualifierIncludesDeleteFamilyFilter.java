/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.QualifierFilter;

public class QualifierIncludesDeleteFamilyFilter extends QualifierFilter {

	public QualifierIncludesDeleteFamilyFilter(CompareOperator op, ByteArrayComparable qualifierComparator) {
		super(op, qualifierComparator);
	}

	@Override
	public ReturnCode filterCell(final Cell c) {
		switch (c.getType()) {
		case DeleteFamily:
			return ReturnCode.INCLUDE;
		default:
			return super.filterCell(c);
		}
	}

}
