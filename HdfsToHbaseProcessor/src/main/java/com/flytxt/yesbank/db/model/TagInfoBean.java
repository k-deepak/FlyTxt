/*Copyright (c) 02-Sep-2016 FlyTxt Corporation and others.
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html

Contributors:
    Deepak  K- initial API and implementation
    
Author : Deepak K
date : 02-Sep-2016

*/

package com.flytxt.yesbank.db.model;

public class TagInfoBean {

	private int tagId;

	private String tableName;

	private String columnFamily;

	private String tagName;

	private String dbType;

	private String defaultValue;

	private String tagType;

	private int modelId;

	private int header;

	// TODO CONVERT INTO TAG HDFS INFO BEAN IF NEEDED.
	private String tagNameValue_hdfs;

	private String customerIdRowKey_hfds;

	public TagInfoBean() {
		super();
	}

	public TagInfoBean(int tagId, String tableName, String columnFamily, String tagName, String dbType,
			String defaultValue, String tagType, int modelId, int header) {
		super();
		this.tagId = tagId;
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		this.tagName = tagName;
		this.dbType = dbType;
		this.defaultValue = defaultValue;
		this.tagType = tagType;
		this.modelId = modelId;
		this.header = header;
	}

	public int getTagId() {
		return tagId;
	}

	public void setTagId(int tagId) {
		this.tagId = tagId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getTagName() {
		return tagName;
	}

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public String getTagType() {
		return tagType;
	}

	public void setTagType(String tagType) {
		this.tagType = tagType;
	}

	public int getModelId() {
		return modelId;
	}

	public void setModelId(int modelId) {
		this.modelId = modelId;
	}

	public int getHeader() {
		return header;
	}

	public void setHeader(int header) {
		this.header = header;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnFamily == null) ? 0 : columnFamily.hashCode());
		result = prime * result + ((dbType == null) ? 0 : dbType.hashCode());
		result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
		result = prime * result + header;
		result = prime * result + modelId;
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + tagId;
		result = prime * result + ((tagName == null) ? 0 : tagName.hashCode());
		result = prime * result + ((tagType == null) ? 0 : tagType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TagInfoBean other = (TagInfoBean) obj;
		if (columnFamily == null) {
			if (other.columnFamily != null)
				return false;
		} else if (!columnFamily.equals(other.columnFamily))
			return false;
		if (dbType == null) {
			if (other.dbType != null)
				return false;
		} else if (!dbType.equals(other.dbType))
			return false;
		if (defaultValue == null) {
			if (other.defaultValue != null)
				return false;
		} else if (!defaultValue.equals(other.defaultValue))
			return false;
		if (header != other.header)
			return false;
		if (modelId != other.modelId)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (tagId != other.tagId)
			return false;
		if (tagName == null) {
			if (other.tagName != null)
				return false;
		} else if (!tagName.equals(other.tagName))
			return false;
		if (tagType == null) {
			if (other.tagType != null)
				return false;
		} else if (!tagType.equals(other.tagType))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TagInfoBean [tagId=" + tagId + ", tableName=" + tableName + ", columnFamily=" + columnFamily
				+ ", tagName=" + tagName + ", dbType=" + dbType + ", defaultValue=" + defaultValue + ", tagType="
				+ tagType + ", modelId=" + modelId + ", header=" + header + "]";
	}

	public String getTagNameValue_hdfs() {
		return tagNameValue_hdfs;
	}

	public void setTagNameValue_hdfs(String tagNameValue_hdfs) {
		this.tagNameValue_hdfs = tagNameValue_hdfs;
	}

	public String getCustomerIdRowKey_hfds() {
		return customerIdRowKey_hfds;
	}

	public void setCustomerIdRowKey_hfds(String customerIdRowKey_hfds) {
		this.customerIdRowKey_hfds = customerIdRowKey_hfds;
	}

}
