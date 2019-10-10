package com.datastax.loyalty.dao;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.loyalty.model.UserPoints;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;

/**
 * Inserts into 2 tables
 * 
 * @author patrickcallaghan
 *
 */
public class CustomerLoyaltyDao {

	private static Logger logger = LoggerFactory.getLogger(CustomerLoyaltyDao.class);

	private static String keyspaceName = "testing";

	private static String pointsTable = keyspaceName + ".user_points";

	private static String INSERT_POINTS = "insert into " + pointsTable
			+ " (id, time, value, comment) values (?,?,?,?);";
	private static String CREATE_CUSTOMER = "insert into " + pointsTable
			+ " (id, time, balance, balanceat) values (?,?,?,?);";
	private static String GET_BALANCE = "select id, balance, balanceat from " + pointsTable + " where id = ?";
	private static String SUM_BALANCE = "select id, sum(value) as value from " + pointsTable
			+ " where id = ? and time > ?";
	private static String UPDATE_BALANCE = "update " + pointsTable
			+ " set balance=?, balanceat=? where id = ? if balance = ?";
	private static String GET_HISTORY = "select * from " + pointsTable + " where id = ?";

	private DseSession session;

	private PreparedStatement createCustomer;
	private PreparedStatement sumBalance;
	private PreparedStatement insertPoints;
	private PreparedStatement updateBalance;
	private PreparedStatement getBalance;
	private PreparedStatement getHistory;

	public CustomerLoyaltyDao(String[] contactPoints) {

		session = DseSession.builder()
				.withCloudSecureConnectBundle("/Users/patrickcallaghan/secure-connect-testing.zip")
				.withAuthCredentials("Patrick", "walrus2005").withKeyspace("testing").build();

		this.createCustomer = session.prepare(CREATE_CUSTOMER);
		this.sumBalance = session.prepare(SUM_BALANCE);
		this.insertPoints = session.prepare(INSERT_POINTS);
		this.updateBalance = session.prepare(UPDATE_BALANCE);
		this.getBalance = session.prepare(GET_BALANCE);
		this.getHistory = session.prepare(GET_HISTORY);
	}

	public void insertPoints(UserPoints cust) {
		session.execute(
				insertPoints.bind("" + cust.getId(), cust.getTime().toInstant(), cust.getValue(), cust.getComment()));
	}

	public void createCustomer(String custid, Date date) {

		session.execute(createCustomer.bind(custid, date.toInstant(), 10, date.toInstant()));
		session.execute(insertPoints.bind(custid, date.toInstant(), 10, "Starting Gift"));
	}

	public UserPoints getBalance(String custid) {
		ResultSet rs = session.execute(getBalance.bind(custid));
		UserPoints loyalty = new UserPoints();

		Row row = rs.one();
		loyalty.setId(row.getString("id"));
		loyalty.setBalance(row.getInt("balance"));
		loyalty.setBalanceat(Date.from(row.getInstant("balanceat")));

		return loyalty;
	}

	public UserPoints sumBalance(String custid, Date date) {
		ResultSet rs = session.execute(sumBalance.bind(custid, date.toInstant()));
		UserPoints loyalty = new UserPoints();

		Row row = rs.one();
		loyalty.setId(custid);
		loyalty.setValue(row.getInt("value"));

		return loyalty;
	}

	public boolean updateBalance(String id, int balance, Date balanceat, int oldBalance) {

		try {
			ResultSet resultSet = this.session.execute(updateBalance.bind(balance, balanceat.toInstant(), id, oldBalance));

			if (resultSet != null) {
				Row row = resultSet.one();
				boolean applied = row.getBoolean(0);

				if (!applied) {
					logger.info("Update failed as balance is " + row.getInt(1) + " and not " + oldBalance);
					return false;
				}
			}
		} catch (WriteTimeoutException e) {
			logger.warn(e.getMessage());
			return false;
		}

		return true;
	}

	public boolean insert(UserPoints cust) {

		try {

			this.session.execute(insertPoints.bind("" + cust.getId(), cust.getTime().toInstant(), cust.getValue(),cust.getComment()));
		} catch (WriteTimeoutException e) {
			logger.warn(e.getMessage());
			return false;
		}

		return true;
	}

	public List<UserPoints> getHistory(String customerid) {

		ResultSet rs = session.execute(getHistory.bind(customerid));
		List<UserPoints> history = new ArrayList<UserPoints>();

		for (Row row : rs.all()) {

			UserPoints loyalty = new UserPoints();
			loyalty.setId(row.getString("id"));
			loyalty.setBalance(row.getInt("balance"));
			loyalty.setBalanceat(Date.from(row.getInstant("balanceat")));
			loyalty.setValue(row.getInt("value"));
			loyalty.setComment(row.getString("comment"));

			history.add(loyalty);
		}

		return history;
	}
}
