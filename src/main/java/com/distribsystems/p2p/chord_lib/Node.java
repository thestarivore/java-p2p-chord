package com.distribsystems.p2p.chord_lib;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Node
 *
 */
public class Node
{
    private String          ipAddr = null;
    private int             port;
    private String          existingNodeIpAddr = null;
    private int             existingNodePort;

    private BigInteger      id;
    //private String          hex;
    private Finger          firstSuccessor;
    private Finger          firstPredecessor;
    private Finger          secondSuccessor;
    private Finger          secondPredecessor;

    private Map<Integer, Finger>        fingerTable = new HashMap<>();
    private Map<BigInteger, String>     itemTable = new HashMap<>();
    private Thread                  server;
    private Semaphore semaphore = new Semaphore(1);

    /**
     * @brief   Chord First Node Constructor: Initializes the first Node in the ChordRing
     * @param   ipAddress     IP Address of the Node
     * @param   portNumber    PORT number of the Node
     */
    public Node(String ipAddress, int portNumber) {
        try {
            //Create a Server Thread
            this.ipAddr = ipAddress;
            this.port   = portNumber;
            this.server = new Server(this);
            this.server.start();

            //Create the Node Identifier by taking the SHA-1 hash function of the IP address and the PORT number
            setNodeId(ipAddress, portNumber);

            // Initialize finger table and successors
            Chord.cLogPrint("Creating a new ChordRing");
            Chord.cLogPrint("Server listening on port " + this.port);
            Chord.cLogPrint("Node's position is " + this.id);
            initFingerTable();
            initSuccessors();

            //Item Generation and Query Testers
            new Thread(new ItemGenerationTester(this)).start();
            new Thread(new ItemQueryTester(this)).start();

            //Run Stabilization protocol
            new Thread(new Stabilizer(this)).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @brief   Chord Successive Node Constructor: Initializes the successive Node in the ChordRing
     * @param   ipAddress             IP Address of the Node
     * @param   portNumber            PORT number of the Node
     * @param   existingNodeIpAddr    IP Address of the already existing Node
     * @param   existingNodePort      PORT number of the already existing Node
     */
    public Node(String ipAddress, int portNumber, String existingNodeIpAddr, int existingNodePort) {
        try {
            //Create a Server Thread
            this.ipAddr = ipAddress;
            this.port   = portNumber;
            this.existingNodeIpAddr = existingNodeIpAddr;
            this.existingNodePort   = existingNodePort;
            this.server = new Server(this);
            this.server.start();

            //Create the Node Identifier by taking the SHA-1 hash function of the IP address and the PORT number
            setNodeId(ipAddress, portNumber);

            // Initialize finger table and successors
            Chord.cLogPrint("Joining an existing ChordRing");
            Chord.cLogPrint("Server listening on port " + this.port);
            Chord.cLogPrint("Connected to the existing node " + this.existingNodeIpAddr + ":" + this.existingNodePort);
            Chord.cLogPrint("Node's position is " + this.id);
            initFingerTable();
            initSuccessors();

            //Item Generation and Query Testers
            new Thread(new ItemGenerationTester(this)).start();
            new Thread(new ItemQueryTester(this)).start();

            //Run Stabilization protocol
            new Thread(new Stabilizer(this)).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @brief   Set/Create the Node Identifier by taking the SHA-1 hash function of the IP address and the PORT number
     * @param   ipAddress     IP Address of the Node
     * @param   portNumber    PORT number of the Node
     */
    void setNodeId(String ipAddress, int portNumber){
        String hex = DigestUtils.sha1Hex(ipAddress + ":" + String.valueOf(portNumber));
        BigInteger baseTwo = BigInteger.valueOf(2L);
        this.id = new BigInteger(hex, 16);
        this.id = this.id.mod(baseTwo.pow(Chord.FINGER_TABLE_SIZE));
        Chord.cLogPrint(this.id.toString());
    }

    /**
     * @brief   Initialize the Finger Table of the Node:
     *              1) If it is the first node in the ChordRing than all fingers will refer to itself; otherwise
     *              2) Create the finger table by contacting the node passed as argument already present on the ring, ask one
     *              finger at a time and wait to get the corresponding node;
     */
    public void initFingerTable() {
        // If this is the first node in the ChordRing
        if (this.existingNodeIpAddr == null) {
            // Initialize all fingers to refer to itself
            for (int i = 0; i < Chord.FINGER_TABLE_SIZE; i++) {
                this.fingerTable.put(i, new Finger(this.ipAddr, this.port));
            }
        } else {
            // Open connection to contact node
            try {
                BigInteger baseTwo = BigInteger.valueOf(2L);
                Socket socket = new Socket(this.existingNodeIpAddr, this.existingNodePort);

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Iterate all the Fingers in the FingerTable
                for (int i = 0; i < Chord.FINGER_TABLE_SIZE; i++) {
                    BigInteger fingerNode = baseTwo.pow(i);
                    fingerNode = fingerNode.add(this.id);

                    // If overflow occur
                    if (fingerNode.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {
                        fingerNode = fingerNode.subtract(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE));
                    }

                    // Send query to chord
                    socketWriter.println(Chord.FIND_FINGER + ":" + fingerNode.toString());
                    Chord.cLogPrint("Sending: " + Chord.FIND_FINGER + ":" + fingerNode.toString());

                    // Read response from chord
                    String serverResponse = socketReader.readLine();

                    // Parse out address and port
                    String[] serverResponseFragments = serverResponse.split(":", 2);
                    String[] addressFragments = serverResponseFragments[1].split(":");

                    // Add response finger to table
                    this.fingerTable.put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));

                    Chord.cLogPrint("Received: " + serverResponse);
                }

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @brief   Initialize the Successors of the Node and Notify the Node that we are it's new predecessor.
     *          We will correct this anyway on the Stabilization protocol if we get it wrongly.                 //TODO: maybe a NEW_SUCCESSOR in case we are the successor might perform better
     */
    private void initSuccessors() {
        this.firstSuccessor = this.fingerTable.get(0);
        this.secondSuccessor = this.fingerTable.get(1);
        this.firstPredecessor = new Finger(this.ipAddr, this.port);
        this.secondPredecessor = new Finger(this.ipAddr, this.port);

        // If we do not open a connection to ourselves --> Notify the first successor that we are the new predecessor
        if (!this.ipAddr.equals(this.firstSuccessor.getIpAddr()) || (this.port != this.firstSuccessor.getPort())) {
            try {
                Socket socket = new Socket(this.firstSuccessor.getIpAddr(), this.firstSuccessor.getPort());

                // Open writer to successor node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);

                // Tell successor that this node is its new predecessor
                socketWriter.println(Chord.NEW_PREDECESSOR + ":" + this.getIpAddr() + ":" + this.getPort());
                Chord.cLogPrint("Sending: " + Chord.NEW_PREDECESSOR + ":" + this.getIpAddr() + ":" + this.getPort() + " to " + this.firstSuccessor.getIpAddr() + ":" + this.firstSuccessor.getPort());

                // Close connections
                socketWriter.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @brief   Print the Status Logs, witch includes the whole FingerTable plus the two successors and the two predecessors
     */
    public void printStatusLogs(){
        Chord.cLogPrint("---------------------------------------");
        Chord.cLogPrint("Node "+this.getId()+":");
        Chord.cLogPrint("---------------------------------------");
        // Print Predecessors and Successors
        Chord.cLogPrint("FirstPredecessor--->" + getFirstPredecessor().getIpAddr() + ":" + getFirstPredecessor().getPort() +
                " (id_= " + getFirstPredecessor().getId().toString() + ")");
        Chord.cLogPrint("SecondPredecessor-->" + getSecondPredecessor().getIpAddr() + ":" + getSecondPredecessor().getPort() +
                " (id_= " + getSecondPredecessor().getId().toString() + ")");
        Chord.cLogPrint("FirstSuccessor----->" + getFirstSuccessor().getIpAddr() + ":" + getFirstSuccessor().getPort() +
                " (id_= " + getFirstSuccessor().getId().toString() + ")");
        Chord.cLogPrint("SecondSuccessor---->" + getSecondSuccessor().getIpAddr() + ":" + getSecondSuccessor().getPort() +
                " (id_= " + getSecondSuccessor().getId().toString() + ")");
        Chord.cLogPrint("---------------------------------------");
        // Iterate all the Fingers in the FingerTable and print them
        BigInteger baseTwo = BigInteger.valueOf(2L);
        for (int i = 0; i < Chord.FINGER_TABLE_SIZE; i++) {
            BigInteger fingerNode = baseTwo.pow(i);
            fingerNode = fingerNode.add(this.id);

            // If overflow occur
            if (fingerNode.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {
                fingerNode = fingerNode.subtract(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE));
            }


            Finger finger = fingerTable.get(i);
            Chord.cLogPrint("Finger " + String.valueOf(i) + "(" + fingerNode + "): " + finger.getIpAddr() + ":" + finger.getPort() + "-->" + finger.getId());
        }
        Chord.cLogPrint("---------------------------------------");
        for(BigInteger key: itemTable.keySet()){
            Chord.cLogPrint("Item: " + key.toString() + " --> '" + itemTable.get(key) + "'");
        }
        Chord.cLogPrint("---------------------------------------");
    }

    /**
     * @brief   Find Item with the specified key in the ChordRing Network
     * @param   key BigInteger key of the item to find
     * @return  String of the response (containing the item in case of success)
     */
    public String findItem(BigInteger key){
        BigInteger baseTwo = BigInteger.valueOf(2L);
        BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
        BigInteger minimumDistance = ringSize;
        BigInteger smallestId = ringSize;
        String response = Chord.NOT_FOUND;
        Finger closestSuccessor = null;
        Finger smallestFinger = null;
        boolean itemIsGreaterThanAllFingers = true;
        boolean nodeIsGreaterThanAllFingers = true;

        // Look for a node identifier in the finger table that is less than the key we are looking for
        // but is also the closest
        for (Finger finger : getFingerTable().values()) {
            BigInteger distance;

            // Find clockwise distance from finger to key
            if (key.compareTo(finger.getId()) <= 0) {
                distance = finger.getId().subtract(key);
            } else {
                distance = ringSize;
            }

            // If the distance we have found is smaller than the current minimum, replace the current minimum
            if (distance.compareTo(minimumDistance) == -1) {
                minimumDistance = distance;
                closestSuccessor = finger;
            }

            // Find at least one finger's id that is greater than the item key
            if(key.compareTo(finger.getId()) < 0){
                itemIsGreaterThanAllFingers = false;
            }

            // Find at least one finger's id that is greater than the node key
            if(this.getId().compareTo(finger.getId()) < 0){
                nodeIsGreaterThanAllFingers = false;
            }

            // If this finger has an id smaller than the smallest we have already register, then set this as the
            // smallest id and save the finger as the smallest
            if(finger.getId().compareTo(smallestId) < 0){
                smallestId = finger.getId();
                smallestFinger = finger;
            }
        }

        // If closest successor is null it means that there is no finger that has an ID greater than the key
        // we are looking for, we should forward the request anyway to the finger with the larger id
        if(closestSuccessor == null){
            BigInteger maxFingerId = new BigInteger("0");
            for (Finger finger : this.getFingerTable().values()) {
                if (maxFingerId.compareTo(finger.getId()) < 0){
                    maxFingerId = finger.getId();
                    closestSuccessor = finger;
                }
            }
        }

        // If the ItemKey is greater than the Node's Id, but the node is already the greatest among fingers, then
        // it should go to the smallest finger
        if(nodeIsGreaterThanAllFingers && itemIsGreaterThanAllFingers && key.compareTo(this.getId()) > 0){
            if(smallestFinger != null)
                closestSuccessor = smallestFinger;
        }

        try {
            // Open socket to chord node
            Socket socket = new Socket(closestSuccessor.getIpAddr(), closestSuccessor.getPort());

            // Open reader/writer to chord node
            PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send query to chord
            socketWriter.println(Chord.FIND_ITEM + ":" + key.toString());
            Chord.cLogPrint("Sent: " + Chord.FIND_ITEM + ":" + key.toString());

            // Read response from chord
            String serverResponse = socketReader.readLine();
            Chord.cLogPrint("Response from node " + closestSuccessor.getIpAddr() + ", port " + closestSuccessor.getPort() + ", position " + " (" + closestSuccessor.getId() + "):");
            Chord.cLogPrint("\n"+serverResponse.toString()+"\n");
            response = serverResponse;

            // Close connections
            socketWriter.close();
            socketReader.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }

    /**
     * @brief   Place an Item with with the specified key in the ChordRing Network
     * @param   item    Item to place on the ChordRing
     * @return  String of the response (containing the node that is storing the item)
     */
    public String placeItem(String item){
        BigInteger itemKey = getKeyOfItem(item);
        return placeItem(itemKey, item);
    }

    /**
     * @brief   Place an Item with with the specified key in the ChordRing Network
     * @param   itemKey BigInteger key of the item to place
     * @param   item    Item to place on the ChordRing
     * @return  String of the response (containing the node that is storing the item)
     */
    public String placeItem(BigInteger itemKey, String item){
        BigInteger baseTwo = BigInteger.valueOf(2L);
        BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
        BigInteger minimumDistance = ringSize;
        String response = Chord.NOT_FOUND;
        Finger closestSuccessor = null;

        // Look for a node identifier in the finger table that is less than the key we are looking for
        // but is also the closest
        for (Finger finger : getFingerTable().values()) {
            BigInteger distance;

            // Find clockwise distance from finger to key
            if (itemKey.compareTo(finger.getId()) <= 0) {
                distance = finger.getId().subtract(itemKey);
            } else {
                distance = ringSize;
            }

            // If the distance we have found is smaller than the current minimum, replace the current minimum
            if (distance.compareTo(minimumDistance) == -1) {
                minimumDistance = distance;
                closestSuccessor = finger;
            }
        }

        // If closest successor is null it means that there is no finger that has an ID greater than the key
        // we are looking for, we should forward the request anyway to the finger with the larger id
        if(closestSuccessor == null){
            BigInteger maxFingerId = new BigInteger("0");
            for (Finger finger : getFingerTable().values()) {
                if (maxFingerId.compareTo(finger.getId()) < 0){
                    maxFingerId = finger.getId();
                    closestSuccessor = finger;
                }
            }
        }

        try {
            // Open socket to chord node
            Socket socket = new Socket(closestSuccessor.getIpAddr(), closestSuccessor.getPort());

            // Open reader/writer to chord node
            PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send query to chord
            socketWriter.println(Chord.PLACE_ITEM + ":" + itemKey.toString() + ":" +  item);
            Chord.cLogPrint("Sent: " + Chord.PLACE_ITEM + ":" + itemKey.toString() + ":" +  item);

            // Read response from chord
            String serverResponse = socketReader.readLine();
            Chord.cLogPrint("Response from node " + closestSuccessor.getIpAddr() + ", port " + closestSuccessor.getPort() + ", position " + " (" + closestSuccessor.getId() + "):");
            response = serverResponse;

            // Close connections
            socketWriter.close();
            socketReader.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }

    /**
     * @brief   Place the Item on the the finger passed as argument
     * @param   finger  finger where to place the item
     * @param   itemKey item's key to place
     * @return  true if the item has been correctly placed, false otherwise
     */
    public boolean placeItem(Finger finger, BigInteger itemKey){
        try {
            // Open socket to chord node
            Socket socket = new Socket(finger.getIpAddr(), finger.getPort());

            // Open reader/writer to chord node
            PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send query to chord
            socketWriter.println(Chord.PLACE_ITEM + ":" + itemKey.toString() + ":" +  getItemTable().get(itemKey));
            Chord.cLogPrint("Sent: " + Chord.PLACE_ITEM + ":" + itemKey.toString() + ":" +  getItemTable().get(itemKey));

            // Read response from chord
            String serverResponse = socketReader.readLine();
            Chord.cLogPrint("Response from node " + finger.getIpAddr() + ", port " + finger.getPort() + ", position " + " (" + finger.getId() + "):");

            //Correct Response --> remove the item from this node
            if(serverResponse != null){
                if(serverResponse.contains(Chord.ITEM_PLACED)){
                    return true;
                }
            }
            else
                return false;

            // Close connections
            socketWriter.close();
            socketReader.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @brief   Create the Identifier(key) by taking the SHA-1 hash function of the item passed as argument
     * @param   item     String of the Item
     */
    public BigInteger getKeyOfItem(String item){
        String hex = DigestUtils.sha1Hex(item);
        BigInteger baseTwo = BigInteger.valueOf(2L);
        BigInteger key = new BigInteger(hex, 16);
        key = key.mod(baseTwo.pow(Chord.FINGER_TABLE_SIZE));
        return key;
    }

    /**
     *  @brief  Empty the ItemTable by delegating(sending) each Item from the ItemTable
     *          to another node in the Finger Table
     */
    public void emptyItemTable() {
        // Iterate all keys in the temp item table, and for each of them send it to another appropriate node
        Map<BigInteger, String> itemTable = new HashMap<>();
        itemTable.putAll(this.getItemTable());                 //Iterate Throughout a copy of the ItemTable for concurrency reasons
        for (BigInteger key : itemTable.keySet()) {
            BigInteger baseTwo = BigInteger.valueOf(2L);
            BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
            BigInteger minimumDistance = ringSize;
            Finger closestSuccessor = null;

            this.acquire();

            /**
             * Look for a node identifier in the finger table that is greater(or equal) than the key we are looking for
             * but is also the closer than the current node
             */
            for (Finger finger : this.getFingerTable().values()) {
                BigInteger distance;

                // Find clockwise distance from finger to query
                if (key.compareTo(finger.getId()) <= 0) {
                    distance = finger.getId().subtract(key);
                } else {
                    distance = ringSize;
                }

                // If the distance we have found is smaller than the current minimum, replace the current minimum
                if (distance.compareTo(minimumDistance) == -1) {
                    minimumDistance = distance;
                    closestSuccessor = finger;
                }
            }

            // If closest successor is null it means that there is no finger that has an ID greater than the key
            // we are looking for, we should forward the request anyway to the finger with the larger id
            if(closestSuccessor == null){
                BigInteger maxFingerId = new BigInteger("0");
                for (Finger finger : this.getFingerTable().values()) {
                    if (maxFingerId.compareTo(finger.getId()) < 0){
                        maxFingerId = finger.getId();
                        closestSuccessor = finger;
                    }
                }
            }

            //Send the item and delete it locally
            if (closestSuccessor != null){
                // Then send the Item to the correct Finger node
                if(placeItem(closestSuccessor, key)){
                    //Correct Response --> remove the item from this node
                    this.getItemTable().remove(key);
                }
            }

            this.release();
        }
    }

    /**
     * @brief   Notify all the nodes in the FingerTable that this node is about to self-destruct
     * @return  None
     */
    public void notifyNodeDestruction(){
        BigInteger baseTwo = BigInteger.valueOf(2L);
        BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
        BigInteger minimumDistance = ringSize;
        List<BigInteger> notifiedFingers = new ArrayList<>();

        this.acquire();

        // Iterate the FingerTable an notify all the nodes (but just ones)
        for (Finger finger : getFingerTable().values()) {
            //Control if the node has already been notified
            if(notifiedFingers.contains(finger.getId()) == false){
                try {
                    // Open socket to chord node
                    Socket socket = new Socket(finger.getIpAddr(), finger.getPort());

                    // Open reader/writer to chord node
                    PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    // Send query to chord
                    socketWriter.println(Chord.FORGET_FINGER + ":" + this.getId());
                    Chord.cLogPrint("Sent: " + Chord.FORGET_FINGER + ":" + this.getId());

                    // Read response from chord
                    String serverResponse = socketReader.readLine();
                    Chord.cLogPrint("Response from node " + finger.getIpAddr() + ", port " + finger.getPort() + ", position " + " (" + finger.getId() + "):");
                    Chord.cLogPrint(serverResponse);

                    // Close connections
                    socketWriter.close();
                    socketReader.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        this.release();
    }

    public Map<Integer, Finger> getFingerTable() {
        return fingerTable;
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public Finger getFirstSuccessor() {
        return firstSuccessor;
    }

    public void setFirstSuccessor(Finger firstSuccessor) {
        this.firstSuccessor = firstSuccessor;
    }

    public Finger getFirstPredecessor() {
        return firstPredecessor;
    }

    public void setFirstPredecessor(Finger firstPredecessor) {
        this.firstPredecessor = firstPredecessor;
    }

    public Finger getSecondSuccessor() {
        return secondSuccessor;
    }

    public void setSecondSuccessor(Finger secondSuccessor) {
        this.secondSuccessor = secondSuccessor;
    }

    public Finger getSecondPredecessor() {
        return secondPredecessor;
    }

    public void setSecondPredecessor(Finger secondPredecessor) {
        this.secondPredecessor = secondPredecessor;
    }

    public String getIpAddr() {
        return ipAddr;
    }

    public int getPort() {
        return port;
    }

    public BigInteger getId() {
        return id;
    }

    public String getExistingNodeIpAddr() {
        return existingNodeIpAddr;
    }

    public void setExistingNodeIpAddr(String existingNodeIpAddr) {
        this.existingNodeIpAddr = existingNodeIpAddr;
    }

    public int getExistingNodePort() {
        return existingNodePort;
    }

    public void setExistingNodePort(int existingNodePort) {
        this.existingNodePort = existingNodePort;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setId(BigInteger id) {
        this.id = id;
    }

    public void setFingerTable(Map<Integer, Finger> fingerTable) {
        this.fingerTable = fingerTable;
    }

    public Map<BigInteger, String> getItemTable() {
        return itemTable;
    }

    public void setItemTable(Map<BigInteger, String> itemTable) {
        this.itemTable = itemTable;
    }

    public void acquire() {
        try {
            this.semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void release() {
        this.semaphore.release();
    }

}
