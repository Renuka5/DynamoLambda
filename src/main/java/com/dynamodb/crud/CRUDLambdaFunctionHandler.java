package com.dynamodb.crud;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;

/**
 * Perform CRUD operations using dynamodb
 *
 */

public class CRUDLambdaFunctionHandler implements RequestHandler<Map<String,String>, Object> {

	//@Override
	public Object handleRequest(Map<String, String> input, Context context) {
        context.getLogger().log("Input Item: " + input);
        
        //create DynamoDB Client
        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        final DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("Student");

        //Write Item to Table in DynamoDb
        writeItemToTable(input, table);
        
        //Read an Item from a table
        readItemFromTable(table);
        
        //Update Item already present in table
        updateItemInTable(table);
        
        //Delete Item from present in table
        deleteItemFromTable(table);
        
        
        return "Success!!";
    }

	private void deleteItemFromTable(Table table) {

	     try {

	         DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("StudentId", 2);

	         table.deleteItem(deleteItemSpec);
	         System.out.println("Deleted Item with StudentId 2");

	     }
	     catch (Exception e) {
	         System.err.println("Error deleting item in " + table.getTableName());
	         System.err.println(e.getMessage());
	     }
	 
		
	}

	private void updateItemInTable(Table table) {

	     try {
	         

	         UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("StudentId", 2)
	             .withReturnValues(ReturnValue.ALL_NEW).withUpdateExpression("set #n = :val1").withNameMap(new NameMap().with("#n", "Name"))
	             .withValueMap(new ValueMap().withString(":val1", "Malathi Mahesh"));

	         UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

	         // Check the response.
	         System.out.println("Printing item after update to name attribute...");
	         System.out.println(outcome.getItem().toJSONPretty());

	     }
	     catch (Exception e) {
	         System.err.println("Error updating item in " + table.getTableName());
	         System.err.println(e.getMessage());
	     }
	 
		
	}

	private void readItemFromTable(Table table) {

	     try {

	         Item item = table.getItem("StudentId", 1);

	         System.out.println("Printing item after retrieving it....");
	         System.out.println(item.toJSONPretty());

	     }
	     catch (Exception e) {
	         System.err.println("GetItem failed in " + table.getTableName());
	         System.err.println(e.getMessage());
	     }

	 
		
	}

	private void writeItemToTable(Map<String,String> input, Table table) {

        try {

            System.out.println("Adding data to " + table.getTableName());

            Item item = new Item().withPrimaryKey("StudentId", Integer.parseInt(input.get("StudentId"))).withString("Name", input.get("Name"))
                .withString("School", input.get("School"));
            
            table.putItem(item);

        }
        catch (Exception e) {
            System.err.println("Failed to create item in " + table.getTableName());
            System.err.println(e.getMessage());
        }
	}
	
}

