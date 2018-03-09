Loyalty Points Demo
========================

This sample application shows how one come write a loyalty points application using DataStax Enterprise.

A user of the loyalty program would collect points from purchases. At some point the user may want to redeem his points against something.

The main points of the application are :

1. Points are collected without ever having to check the balance.
2. When a user wants to redeem points we 
	1. update the balance to be the current balance. 
	2. Then we have enough points to redeem the amount requested.
3. If the user has enough points, the we reduce the balance and insert the redeem event. This is done in a batch to avoid any concurrency issues.

To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
	
To create some loyalty points and redeems, run the following 
	
	mvn exec:java -Dexec.mainClass="com.datastax.loyalty.Main"  -DcontactPoints=localhost

To get the current (live) balance of any user.  
```
select sum(value) from user_points where id = '0';
```

To start the webservice
```
mvn jetty:run
```

The Rest API commands are

#### Create Customer 
```
/createcustomer/{customerid}

eg

http://localhost:8080/datastax-loyalty-demo/rest/createcustomer/U123141412
```

#### Add Points 
```
/addpoints/{customerid}/{points}

eg

http://localhost:8080/datastax-loyalty-demo/rest/addpoints/U123141412/1
```

#### Redeem Points 
```
/redeempoints/{customerid}/{points}

eg

http://localhost:8080/datastax-loyalty-demo/rest/redeempoints/U123141412/5
```

#### Get Balance 
```
/getbalance/{customerid}

eg

http://localhost:8080/datastax-loyalty-demo/rest/getbalance/U123141412
```

#### Get History 
```
/gethistory/{customerid}

eg

http://localhost:8080/datastax-loyalty-demo/rest/gethistory/U123141412
```


To remove the tables and the schema, run the following.

    mvn exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
