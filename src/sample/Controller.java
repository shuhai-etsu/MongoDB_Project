package sample;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import java.awt.*;
import java.awt.Color;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import javax.swing.*;
import java.util.*;
import java.text.*;
public class Controller {
    @FXML
    private Button server1;

    @FXML
    private Button server2;

    @FXML
    private Button server3;

    @FXML
    private Button button1;

    @FXML
    private Button button2;

    @FXML
    private Button button3;

    @FXML
    private TextArea textArea;
    String output = null;
    MongoDatabase db = null;
    MongoCollection<Document> collection1 = null;
    MongoCollection<Document> collection2 = null;
    MongoCollection<Document> collection3 = null;

    public void server1ButtonListener(){
        try  {
            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient("151.141.134.151", 27017);
            output =  "Connected to remote MongoDB server at 151.141.134.151 successfully";
            textArea.setText(output);

            // Now connect to your databases
            db = mongoClient.getDatabase("mydb");
            output = output + "\nConnected to database-mydb- successfully";
            textArea.setText(output);
            textArea.setWrapText(true);
        }

        catch (Exception e) {

            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }

        //access collection mycol
        collection1 = db.getCollection("mycol");
        output = output+"\nget collection mycol";
        textArea.setText(output);
    }

    public void server2ButtonListener(){
        try {
            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient("151.141.135.44", 27017);
            output = output + "\nConnected to remote MongoDB server at 151.141.135.44 successfully";
            textArea.setText(output);

            // Now connect to your databases
            db = mongoClient.getDatabase("mydb");
            output = output + "\nConnected to database-mydb- successfully";
            textArea.setText(output);

        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }

        //access collection mycol
        collection2 = db.getCollection("mycol");
        output = output+"\nget collection mycol";
        textArea.setText(output);
    }

    public void server3ButtonListener(){
        try {
            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient("151.141.134.236", 27017);
            output = output+ "\nConnected to remote MongoDB server at 151.141.134.236 successfully";
            textArea.setText(output);

            // Now connect to your databases
            db = mongoClient.getDatabase("mydb");
            output = output + "\nConnected to database-mydb- successfully";
            textArea.setText(output);

        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        //access collection mycol
        collection3 = db.getCollection("mycol");
        output = output+"\nget collection mycol";
        textArea.setText(output);
    }


    /*
     *Button 1: generate documents randomly
     */
    public void button1ButtonListener() {

        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");;
        Date today = new Date();


        //Create a document
        Document doc = new Document("title", "My first reddit blog post")
                .append("author", "shuhai")
                .append("content", "This is my first blog I have ever written")
                .append("Date created",formatter.format(today));
        //output = output+"\nCreated one document";
        //textArea.setText(output);

        //Insert the document to collection test
        collection1.insertOne(doc);

        //Create a list of documents and insert them into collection

        List<Document> documents = new ArrayList<Document>();
        for (int i = 2; i <=100; i++) {
            doc = new Document("title", "My "+i+"th reddit blog post")
                    .append("author", "shuhai")
                    .append("content", "This is my" +i+ "th blog I have ever written")
                    .append("Date created",formatter.format(today));
            documents.add(doc);
        }
        collection1.insertMany(documents);
        output = output + "\n"+collection1.count() +" documents inserted in the collection mycol at primary node";
        textArea.setText(output);
    }



    /*
     *Button 2: query documents from servers
     */
    public void button2ButtonListener()
    {
        MongoCursor<Document> cursor = null;
        int selection;
        selection = Integer.parseInt(JOptionPane.showInputDialog("From which server to fetch documents?"));
        switch (selection)
        {
            case 1:
                cursor = collection1.find().iterator();break;
            case 2:
                cursor = collection2.find().iterator();break;
            case 3:
                cursor = collection3.find().iterator();break;
            default:
                cursor = collection1.find().iterator();
        }
        output ="The documents fetched from server"+selection+":\n";
        try {
            while (cursor.hasNext()) {
                output = output +"\n"+cursor.next().toJson();
            }
        } finally {
            cursor.close();
        }
        textArea.clear();
        textArea.setText(output);

        //collection.drop();
    }



/*
 *Button 3: check consistency between different servers
 */
    public void button3ButtonListener(){
        boolean isConsistent;

        isConsistent = checkConsistency();
        isConsistent = true;
        if (isConsistent){
            output = "\nThe collections from three servers are consistent";
        }else{
            output = "\nThe collections from three servers are not consistent";
        }
        //textArea.clear();
        textArea.setText(output);
        //textArea.clear();
    }

    public boolean checkConsistency(){
        boolean isConsistent = true;
        MongoCursor<Document> cursor1 = collection1.find().iterator();
        MongoCursor<Document> cursor2 = collection2.find().iterator();
        MongoCursor<Document> cursor3 = collection3.find().iterator();

        if (collection1.count() != collection2.count() || collection2.count() != collection3.count()){
            isConsistent = false;
            System.out.println("something wrong here at counting");
            return isConsistent;
        }
        Object obj1 = null;
        Object obj2 = null;
        Object obj3 = null;

        try {
            while (cursor1.hasNext()) {
//                obj1 = cursor1.next().toJson();
//                obj2 = cursor2.next().toJson();
//                obj3 = cursor3.next().toJson();

                obj1 = cursor1.next();
                obj2 = cursor2.next();
                obj3 = cursor3.next();
                isConsistent = obj1.equals(obj2) && obj2.equals(obj3) && isConsistent;
                if (isConsistent == false){
                    System.out.println("something wrong here");
                    return isConsistent;
                }
            }
        } finally {
            cursor1.close();
            cursor2.close();
            cursor3.close();
        }
        //isConsistent=true;
        return isConsistent;
    }
}