package com.datastax.loyalty;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.loyalty.model.UserPoints;
import com.datastax.loyalty.service.LoyaltyService;

public class CustomerLoyaltyWriter implements KillableRunner {

	private static Logger logger = LoggerFactory.getLogger(CustomerLoyaltyWriter.class);
	private volatile boolean shutdown = false;
	private LoyaltyService service;
	private BlockingQueue<UserPoints> queue;

	public CustomerLoyaltyWriter(LoyaltyService service, BlockingQueue<UserPoints> queue) {
		this.service = service;
		this.queue = queue;
	}

	@Override
	public void run() {
		UserPoints customerLoyalty;
		while (!shutdown) {
			customerLoyalty = queue.poll();

			if (customerLoyalty != null) {
				try {					
					//Redeem will have a minus value on the customers loyalty acccount. 
					if (customerLoyalty.getValue() < 0) {
						service.redeem(customerLoyalty);
						
					}else{
						service.addPoints(customerLoyalty.getId(), customerLoyalty.getTime(), customerLoyalty.getValue(), customerLoyalty.getComment());
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void shutdown() {
		while (!queue.isEmpty())
			logger.info("Finishing");
			shutdown = true;
	}
}
