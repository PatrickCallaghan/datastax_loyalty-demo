package com.datastax.loyalty.model;

import java.util.Date;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;


@Entity
public class UserPoints {

	@PartitionKey
	private String id;
	@ClusteringColumn
	private Date time;

	private int balance;
	private Date balanceat;
	private int value; 
	private String comment;
	
	public UserPoints(){}
	
	public UserPoints(String id, Date time, int value, String comment) {
		super();
		this.id = id;
		this.time = time;
		this.value = value;
		this.comment = comment;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public int getBalance() {
		return balance;
	}
	public void setBalance(int balance) {
		this.balance = balance;
	}
	public Date getBalanceat() {
		return balanceat;
	}
	public void setBalanceat(Date balanceat) {
		this.balanceat = balanceat;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "CustomerLoyalty [id=" + id + ", time=" + time + ", balance=" + balance + ", balanceat=" + balanceat
				+ ", value=" + value + ", comment=" + comment + "]";
	}
}
