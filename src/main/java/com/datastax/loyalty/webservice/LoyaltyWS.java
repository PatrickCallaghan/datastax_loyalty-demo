package com.datastax.loyalty.webservice;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.loyalty.dao.NotEnoughPointsException;
import com.datastax.loyalty.model.UserPoints;
import com.datastax.loyalty.service.LoyaltyService;

@WebService
@Path("/")
public class LoyaltyWS {

	private Logger logger = LoggerFactory.getLogger(LoyaltyWS.class);
	//private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd"); //from = new DateTime(inputDateFormat.parse(fromDate));

	//Service Layer.
	private LoyaltyService service = new LoyaltyService();
	
	@GET
	@Path("/createcustomer/{customerid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createCustomer(@PathParam("customerid") String customerid) {
		
		service.createCustomer(customerid, new Date());
		
		logger.info("Returned response");
		return Response.status(Status.OK).entity("Customer " + customerid + " created").build();
	}
	
	@GET
	@Path("/addpoints/{customerid}/{points}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response addPoints(@PathParam("customerid") String customerid,@PathParam("points") int points) {
		
		service.addPoints(customerid, new Date(), points, "Adding points");
		
		return Response.status(Status.OK).entity(points + " added to customer " + customerid + "'s account").build();	
	}
	
	@GET
	@Path("/redeempoints/{customerid}/{points}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response redeemPoints(@PathParam("customerid") String customerid,@PathParam("points") int points) {
		
		try {
			service.redeemPoints(customerid, new Date(), points, "Redeeming");
			return Response.status(Status.OK).entity(points + " redeemed from customer " + customerid + "'s account").build();
		} catch (NotEnoughPointsException e) {
			logger.error(e.getMessage());
			return Response.status(Status.CONFLICT).entity(e.getMessage()).build();	
		}
	}
	
	@GET
	@Path("/getbalance/{customerid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getBalance(@PathParam("customerid") String customerid) {
		
		UserPoints balance = service.getBalance(customerid);
		UserPoints sumBalance = service.sumBalance(customerid, balance.getBalanceat());
		
		return Response.status(Status.OK).entity(sumBalance.getValue() + balance.getBalance()).build();	
	}
	
	@GET
	@Path("/gethistory/{customerid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getHistory(@PathParam("customerid") String customerid) {
		
		List<UserPoints> history = service.getHistory(customerid);
		
		return Response.status(Status.OK).entity(history).build();	
	}
}
