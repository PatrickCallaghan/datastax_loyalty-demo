package com.datastax.loyalty.service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.loyalty.dao.CustomerLoyaltyDao;
import com.datastax.loyalty.dao.NotEnoughPointsException;
import com.datastax.loyalty.model.UserPoints;

public class LoyaltyService {
	private static Logger logger = LoggerFactory.getLogger(LoyaltyService.class);
	private CustomerLoyaltyDao dao;

	public LoyaltyService(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new CustomerLoyaltyDao(contactPointsStr.split(","));
	}
	
	public void addPoints(String id, Date date, int points, String comment){
		
		UserPoints cust = new UserPoints(id, date, points, comment);		
		dao.insertPoints(cust);
	}
	
	public void redeemPoints(String id, Date date, int points, String comment) throws NotEnoughPointsException{
		
		UserPoints cust = new UserPoints(id, date, -points, comment);
		
		redeem(cust);
	}

	public void redeem(UserPoints customerLoyalty) throws NotEnoughPointsException {
		// Redeem			
		
		//Update the balance before we redeem.
		UserPoints balance = this.dao.getBalance(customerLoyalty.getId());						
		UserPoints sumBalance = this.dao.sumBalance(customerLoyalty.getId(), balance.getBalanceat());
		
		int currentBalance = balance.getBalance();		
		int balanceSince = sumBalance.getValue();						
		int newBalance = currentBalance + balanceSince;
		
		logger.info(customerLoyalty.toString());
		
		//Balance when we redeem the points
		int redeemedBalance = newBalance + customerLoyalty.getValue();
		
		//If enough points - then redeem the balance. 
		if (redeemedBalance >= 0){						

			dao.insert(customerLoyalty);
			dao.updateBalance(customerLoyalty.getId(), redeemedBalance, customerLoyalty.getTime(), currentBalance);

		}else{
			String msg = "Cannot redeem " + (-customerLoyalty.getValue()) + " points as balance = " + newBalance + " for customer " + customerLoyalty.getId();
			throw new NotEnoughPointsException(msg);
		}
	}

	
	public void createCustomer(String id, Date date){
		
		dao.createCustomer(id, date);
	}
	
	public UserPoints getBalance(String id){
		return dao.getBalance(id);
	}

	public UserPoints sumBalance(String id, Date lastBalanceAt){
		return dao.sumBalance(id, lastBalanceAt);
	}

	public List<UserPoints> getHistory(String customerid) {
		return dao.getHistory(customerid);
	}
}

