package com.distribsystems.p2p;


// Java implementation for a client
// Save file as Client.java

import com.distribsystems.p2p.chord_lib.Chord;
import com.distribsystems.p2p.chord_lib.Node;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

// Client class
public class Client
{
    public static void main(String[] args){
        Node node;
        String ip = "127.0.0.1";
        int port = 8000;

        /**
         * Program Arguments
         * args[0] = port of the newly created Node
         * args[1] = ip address of the Node to connect to
         * args[2] = port of the Node to connect to
         * args[3] = create a node
         */
        if(args.length > 0){
            port = Integer.parseInt(args[0]);
        }

        if (args.length == 1) {
            // Create new node
            node = new Node(ip, port);
        } else if (args.length == 3) {
            String existingIpAddress = args[1];
            int existingPort = Integer.parseInt(args[2]);

            // Create new node
            node = new Node(ip, port, existingIpAddress, existingPort);
        } else {
            // User Input
            Scanner myObj = new Scanner(System.in);

            System.out.println("Create a Node within a new ChordRing? [y/n]\n");
            String resp = myObj.nextLine();
            //New Chord Ring
            if(resp.equals("y")){
                System.out.println("\n Insert Node's PortNumber: ");
                port = myObj.nextInt();
                myObj.nextLine();

                // Create new node
                node = new Node(ip, port);

                //Choose Mode
                Chord.setEnableLogs(false);
                System.out.println("\nChoose between:");
                System.out.println("[1] Run the Node with it's logs.");
                System.out.println("[2] Run a Client, while running the Node in background.");
                int mode = myObj.nextInt();
                myObj.nextLine();
                if (mode == 1) {
                    Chord.setEnableLogs(true);
                }else if (mode == 2) {
                    Chord.setEnableLogs(false);
                    runClientAutoma(node);
                }
            }
            //Add Node to Existing ChordRing
            else{
                System.out.println("\n Insert Node's PortNumber: ");
                port = myObj.nextInt();
                myObj.nextLine();

                System.out.println("\n Insert the IP Address of the already existing Node: ");
                String existingIpAddress = myObj.nextLine();

                System.out.println("\n Insert the PortNumber of the already existing Node: ");
                int existingPort = myObj.nextInt();
                myObj.nextLine();

                // Create new node
                node = new Node(ip, port, existingIpAddress, existingPort);

                //Choose Mode
                Chord.setEnableLogs(false);
                System.out.println("\nChoose between:");
                System.out.println("[1] Run the Node with it's logs.");
                System.out.println("[2] Run a Client, while running the Node in background.");
                int mode = myObj.nextInt();
                myObj.nextLine();
                if (mode == 1) {
                    Chord.setEnableLogs(true);
                }else if (mode == 2) {
                    Chord.setEnableLogs(false);
                    runClientAutoma(node);
                }
            }
        }
    }

    /**
     * @brief   Running the automa of the client
     */
    private static void runClientAutoma(Node node){
        String query = "";
        String resp = "";
        System.out.println("------------CLIENT-----------\n");
        Scanner myObj = new Scanner(System.in);

        while (!query.equals("exit")){
            System.out.println("---------------------------------------");
            Map<BigInteger, String> items = new HashMap<>();
            items.putAll(node.getItemTable());
            if(!items.isEmpty()){
                System.out.println("Items on this node:");
            }
            for(BigInteger key: items.keySet()){
                System.out.println("Item: " + key.toString() + " --> '" + items.get(key) + "'");
            }
            System.out.println("---------------------------------------");

            System.out.println("\nMenu:");
            System.out.println("'put'   --> Put/Place an item on the ChordRing");
            System.out.println("'get'   --> Get/Query an item by Key;");
            System.out.println("'exit'  --> Exit and close node;");
            query = myObj.nextLine();

            if(query.equals("put")){
                System.out.println("Insert Item(String) to place on the network:");
                String item = myObj.nextLine();
                node.acquire();
                resp = node.placeItem(item);
                System.out.println("Response: " + resp);
                node.release();
            }else if(query.equals("get")){
                System.out.println("Insert Key of the Item to retrieve form the network:");
                String key = myObj.nextLine();
                node.acquire();
                resp = node.findItem(new BigInteger(key));
                System.out.println("Response: " + resp);
                node.release();
            }else if(query.equals("exit")){
                System.exit(0);
            }else{
                System.out.println("\nWrong input, try again..:\n");
            }
        }
    }
}

