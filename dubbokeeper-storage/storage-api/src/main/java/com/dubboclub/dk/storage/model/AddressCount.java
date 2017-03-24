package com.dubboclub.dk.storage.model;

import java.io.Serializable;

/**
 * Created by fei on 2017/3/24.
 */
public class AddressCount implements Serializable{

	private String application;

	private String remoteAddress;

	private Integer count;

	public AddressCount() {
	}

	public AddressCount(String application, Integer count) {
		this.application = application;
		this.count = count;
	}

	public String getApplication() {
		return application;
	}

	public void setApplication(String application) {
		this.application = application;
	}

	public String getRemoteAddress() {
		return remoteAddress;
	}

	public void setRemoteAddress(String remoteAddress) {
		this.remoteAddress = remoteAddress;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}
}
