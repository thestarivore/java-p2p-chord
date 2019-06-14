package com.distribsystems.p2p.chord_lib;

// Java implementation of Server side
// It contains two classes : Server and ClientHandler
// Save file as Server.java

import java.io.*;
import java.math.BigInteger;
import java.net.*;

// Server class
public class Server extends Thread
{
    private int port;
    private ServerSocket ss;
    private Node node;

    /**
     * Node's Server Socket Constructor: Basically a Listener that is always waiting for commands
     * from the other nodes, interprets them and takes action
     * @param node  The Node owning the Server Socket
     * @throws IOException
     */
    public Server(Node node) throws IOException {
        this.node = node;
        port = node.getPort();
        ss = null;
    }

    @Override
    public void run() {
        do {
            try {
                ss = new ServerSocket(port);
            } catch (Exception e) {
                Chord.cLogPrint("Address already used (" + port + ") \n");
                port++;
            }
        }while (ss == null);

        // running infinite loop for getting
        // client request
        while (true)
        {
            Socket s = null;

            //Reopen the ServerSocket, if by any chance it's closed
            if(ss.isClosed()){
                try {
                    ss = new ServerSocket(port);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try
            {
                // socket object to receive incoming client requests
                s = ss.accept();

                Chord.cLogPrint("A new client is connected : " + s);

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                Chord.cLogPrint("Assigning new thread for this client");

                // create a new thread object
                Thread t = new ClientHandler(s, dis, dos, node);

                // Invoking the start() method
                t.start();
            }
            catch (Exception e){
                try {
                    s.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    }

    public int getPort(){
        return port;
    }

    public boolean isUp(){
        if(ss != null){
            if(ss.isBound())
                return true;
            else
                return false;
        }
        return  false;
    }
}

// ClientHandler class
class ClientHandler extends Thread
{
    final DataInputStream dis;
    final DataOutputStream dos;
    final Socket s;
    private Node node;


    // Constructor
    public ClientHandler(Socket s, DataInputStream dis, DataOutputStream dos, Node node)
    {
        this.s = s;
        this.dis = dis;
        this.dos = dos;
        this.node = node;
    }

    @Override
    public void run()
    {
        try {
            // Create socket readers and writers
            PrintWriter socketWriter = new PrintWriter(this.s.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(this.s.getInputStream()));

            // Read the query from the client and send back a response
            String query;
            while ((query = socketReader.readLine()) != null) {
                //The Command and the Content are splinted by a : character
                String[] queryElements = query.split(":", 2);
                String command = queryElements[0];
                String content = queryElements[1];
                Chord.cLogPrint("Received: " + command + " " + content);

                switch (command) {
                    case Chord.FIND_FINGER: {
                        String response = this.findFinger(content);
                        Chord.cLogPrint("Sent: " + response);

                        // Respond back to the client
                        socketWriter.println(response);

                        break;
                    }
                    case Chord.FORGET_FINGER: {
                        String response = this.forgetFinger(content);
                        Chord.cLogPrint("Sent: " + response);

                        // Respond back to the client
                        socketWriter.println(response);

                        break;
                    }
                    case Chord.NEW_PREDECESSOR: {
                        // Parse address and port from the message received
                        String[] contentFragments = content.split(":");
                        String address = contentFragments[0];
                        int port = Integer.valueOf(contentFragments[1]);

                        // Acquire lock
                        this.node.acquire();

                        // Move fist predecessor to second
                        this.node.setSecondPredecessor(this.node.getFirstPredecessor());

                        // Set first predecessor to new finger received in message
                        this.node.setFirstPredecessor(new Finger(address, port));

                        //Logs
                        node.printStatusLogs();

                        // Release lock
                        this.node.release();

                        break;
                    }
                    case Chord.REQUEST_PREDECESSOR: {
                        // Return the first predecessor address:port
                        String response = this.node.getFirstPredecessor().getIpAddr() + ":" + this.node.getFirstPredecessor().getPort();
                        Chord.cLogPrint("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }

                    case Chord.PING: {
                        // Reply to the ping
                        String response = Chord.PONG;
                        Chord.cLogPrint("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }

                    case Chord.FIND_ITEM: {
                        String response = this.findItemByKey(content);
                        Chord.cLogPrint("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }

                    case Chord.PLACE_ITEM: {
                        // Parse key and item from the message received
                        String[] contentFragments = content.split(":");
                        String key = contentFragments[0];
                        String item = contentFragments[1];

                        String response = this.placeItem(key, item);
                        Chord.cLogPrint("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }
                }
            }

            try
            {
                // closing resources
                this.dis.close();
                this.dos.close();

            }catch(IOException e){
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //}
    }

    /**
     * @brief   Find the Node most suitable for the finger queried. Get the node with the smaller id but greater than the
     *          node's id. Also ask this node for a nearer node still if any.
     * @param   id  Finger's identification
     * @return  The message to send back of the form FINGER_FOUND:XXX.XXX.XXX.XXX:PPPP if it found a valid candidate,
     *          "NOT_FOUND" otherwise
     */
    private String findFinger(String id) {
        BigInteger queryId = new BigInteger(id);
        String response = Chord.NOT_FOUND;

        // Wrap the queryid if it is as big as the ring
        if (queryId.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {              //TODO:togliere
            queryId = queryId.subtract(new BigInteger(String.valueOf(Chord.FINGER_TABLE_SIZE)));
        }

        // If the query is greater than our predecessor id and less than equal to our id then we have the value
        if (this.doesIdReferToCurrentNode(queryId)) {
            response = Chord.FINGER_FOUND + ":" +  this.node.getIpAddr() + ":" + this.node.getPort();
        } else if(this.doesIdReferToNextNode(queryId)) {
            response = Chord.FINGER_FOUND + ":" +  this.node.getFirstSuccessor().getIpAddr() + ":" + this.node.getFirstSuccessor().getPort();
        } else if(this.doesIdReferToNextNextNode(queryId)) {
            response = Chord.FINGER_FOUND + ":" +  this.node.getSecondSuccessor().getIpAddr() + ":" + this.node.getSecondSuccessor().getPort();
        }else { // We don't have the query so we must search our fingers for it
            BigInteger baseTwo = BigInteger.valueOf(2L);
            BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
            BigInteger minimumDistance = ringSize;
            Finger closestPredecessor = null;

            this.node.acquire();

            // Look for a node identifier in the finger table that is less than the key we are looking for
            // but is also the closest
            for (Finger finger : this.node.getFingerTable().values()) {
                BigInteger distance;

                // Find clockwise distance from finger to query
                if (queryId.compareTo(finger.getId()) >= 0) {
                    distance = queryId.subtract(finger.getId());
                } else {
                    distance = queryId.add(ringSize.subtract(finger.getId()));
                }

                // If the distance we have found is smaller than the current minimum, replace the current minimum
                if (distance.compareTo(minimumDistance) == -1) {
                    minimumDistance = distance;
                    closestPredecessor = finger;
                }
            }

            Chord.cLogPrint("queryid: " + queryId + "distance: " + minimumDistance + " on " + closestPredecessor.getIpAddr() + ":" + closestPredecessor.getPort());

            try {
                // Open socket to chord node
                Socket socket = new Socket(closestPredecessor.getIpAddr(), closestPredecessor.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.FIND_FINGER + ":" + queryId);
                Chord.cLogPrint("Sent: " + Chord.FIND_FINGER + ":" + queryId);

                // Read response from chord
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Response from node " + closestPredecessor.getIpAddr() + ", port " + closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getId() + "):");

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

            this.node.release();
        }

        return response;
    }

    /**
     * @brief   Forget the node, by deleting it from the the FingerTable and predecessors
     * @param   id  Finger's identification
     * @return  The message to send back of the form FINGER_FOUND:XXX.XXX.XXX.XXX:PPPP if it found a valid candidate,
     *          "NOT_FOUND" otherwise
     */
    private String forgetFinger(String id) {
        BigInteger queryId = new BigInteger(id);
        BigInteger baseTwo = BigInteger.valueOf(2L);
        BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
        BigInteger minimumDistance = ringSize;
        Finger closestPredecessor = null;

        this.node.acquire();

        // Search for the node notified in the FingerTable
        for (Finger finger : this.node.getFingerTable().values()) {
            if(finger.getId().compareTo(queryId) == 0){
                finger.setId(node.getId());
                finger.setIpAddr(node.getIpAddr());
                finger.setPort(node.getPort());
            }
        }

        //Test Predecessors
        if(node.getFirstPredecessor().getId().compareTo(queryId) == 0){
            //If different use the second predecessor
            if(node.getSecondPredecessor().getId().compareTo(queryId) != 0){
                node.setFirstPredecessor(node.getSecondPredecessor());
            }else{
                // Search for a finger to be used as predecessor
                for (Finger finger : this.node.getFingerTable().values()) {
                    BigInteger distance;

                    // Find clockwise distance from finger to node id
                    if (node.getId().compareTo(finger.getId()) >= 0) {
                        distance = node.getId().subtract(finger.getId());
                    } else {
                        distance = node.getId().add(ringSize.subtract(finger.getId()));
                    }

                    // If the distance we have found is smaller than the current minimum, replace the current minimum
                    if (distance.compareTo(minimumDistance) == -1) {
                        minimumDistance = distance;
                        closestPredecessor = finger;
                    }
                }
                node.setFirstPredecessor(closestPredecessor);
                node.setSecondPredecessor(closestPredecessor);
            }
        }

        //Test Successors
        if(node.getFirstSuccessor().getId().compareTo(queryId) == 0){
            node.setFirstSuccessor(node.getFingerTable().get(0));
            if(node.getSecondSuccessor().getId().compareTo(queryId) == 0) {
                node.setSecondSuccessor(node.getFingerTable().get(1));
            }
        }

        Chord.cLogPrint("Node " + queryId.toString() + " has been removed from the finger table and predecessors..");
        this.node.release();

        return Chord.FINGER_FORGOTTEN;
    }

    /**
     * @brief   Find the Item with the key we are passed as argument. Get the key from the node with the smaller id but greater than the key.
     * @param   key  Item's Key
     * @return  The message to send back of the form ITEM_FOUND:... if it has found a valid candidate, "NOT_FOUND" otherwise
     */
    private String findItemByKey(String key) {
        BigInteger queryId = new BigInteger(key);
        String response = Chord.NOT_FOUND;

        // Wrap the queryid if it is as big as the ring
        if (queryId.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {
            queryId = queryId.subtract(new BigInteger(String.valueOf(Chord.FINGER_TABLE_SIZE)));
        }

        // If the query is greater than our predecessor id and less than equal to our id then we have the value
        if (this.doesIdReferToCurrentNode(queryId)) {
            response = Chord.ITEM_FOUND + ":" + this.node.getIpAddr() + ":" + this.node.getPort() +
                    ":" + this.node.getItemTable().get(queryId);
        } else if (this.doesIdReferToNextNode(queryId)) {
            this.node.acquire();

            try {
                // Open socket to chord node
                Socket socket = new Socket(this.node.getFirstSuccessor().getIpAddr(), this.node.getFirstSuccessor().getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.FIND_ITEM + ":" + key);
                Chord.cLogPrint("Sent: " + Chord.FIND_ITEM + ":" + key);

                // Read response from chord
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Response from node " + this.node.getFirstSuccessor().getIpAddr() + ", port " + this.node.getFirstSuccessor().getPort() + ", position " + " (" + this.node.getFirstSuccessor().getId() + "):");

                response = serverResponse;

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            this.node.release();
        } else if (this.doesIdReferToNextNextNode(queryId)) {
            this.node.acquire();

            try {
                // Open socket to chord node
                Socket socket = new Socket(this.node.getSecondSuccessor().getIpAddr(), this.node.getSecondSuccessor().getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.FIND_ITEM + ":" + key);
                Chord.cLogPrint("Sent: " + Chord.FIND_ITEM + ":" + key);

                // Read response from chord
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Response from node " + this.node.getSecondSuccessor().getIpAddr() + ", port " + this.node.getSecondSuccessor().getPort() + ", position " + " (" + this.node.getSecondSuccessor().getId() + "):");

                response = serverResponse;

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            this.node.release();
        } else { // We don't have the query so we must search our fingers for it
            BigInteger baseTwo = BigInteger.valueOf(2L);
            BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
            BigInteger minimumDistance = ringSize;
            Finger closestPredecessor = null;

            this.node.acquire();

            // Look for a node identifier in the finger table that is less than the key id and closest in the ID space to the key id
            for (Finger finger : this.node.getFingerTable().values()) {
                BigInteger distance;

                // Find clockwise distance from finger to query
                if (queryId.compareTo(finger.getId()) >= 0) {
                    distance = queryId.subtract(finger.getId());
                } else {
                    distance = queryId.add(ringSize.subtract(finger.getId()));
                }

                // If the distance we have found is smaller than the current minimum, replace the current minimum
                if (distance.compareTo(minimumDistance) == -1) {
                    minimumDistance = distance;
                    closestPredecessor = finger;
                }
            }

            Chord.cLogPrint("queryid: " + queryId + " minimum distance: " + minimumDistance + " on " + closestPredecessor.getIpAddr() + ":" + closestPredecessor.getPort());

            try {
                // Open socket to chord node
                Socket socket = new Socket(closestPredecessor.getIpAddr(), closestPredecessor.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.FIND_ITEM + ":" + key);
                Chord.cLogPrint("Sent: " + Chord.FIND_ITEM + ":" + key);

                // Read response from chord
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Response from node " + closestPredecessor.getIpAddr() + ", port " + closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getId() + "):");

                response = serverResponse;

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            this.node.release();
        }

        return response;
    }

    /**
     * @brief   Find the Item with the key we are passed as argument. Get the key from the node with the smaller id but greater than the key.
     * @param   key  Item's Key
     * @param   item Item's String
     * @return  The message to send back of the form ITEM_FOUND:... if it has found a valid candidate, "NOT_FOUND" otherwise
     */
    private String placeItem(String key, String item) {
        BigInteger itemKey = new BigInteger(key);
        String response = Chord.NOT_FOUND;

        // Wrap the ItemKey if it is as big as the ring
        if (itemKey.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {
            itemKey = itemKey.subtract(new BigInteger(String.valueOf(Chord.FINGER_TABLE_SIZE)));
        }

        // If the query is greater than our predecessor id and less than equal to our id then we have the value
        if (this.shouldItemBeStoredOnCurrentNode(itemKey)) {
            // Add the Item on the ItemTable and send back the feedback
            this.node.getItemTable().put(new BigInteger(key), item);
            response = Chord.ITEM_PLACED + ":" + node.getId().toString();
            Chord.cLogPrint("PlacedItem: key=" + key + ", item=" + item);
        } else { // We don't have the query so we must search our fingers for it
            BigInteger baseTwo = BigInteger.valueOf(2L);
            BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
            BigInteger minimumDistance = ringSize;
            BigInteger smallestId = ringSize;
            Finger closestSuccessor = null;
            Finger smallestFinger = null;
            boolean itemIsGreaterThanAllFingers = true;
            boolean nodeIsGreaterThanAllFingers = true;

            this.node.acquire();

            // Look for a node identifier in the finger table that is less than the key id and closest in the ID space to the key id
            for (Finger finger : this.node.getFingerTable().values()) {
                BigInteger distance;

                // Find clockwise distance from finger to query
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

                // Find at least one finger's id that is greater than the item key
                if(itemKey.compareTo(finger.getId()) < 0){
                    itemIsGreaterThanAllFingers = false;
                }

                // Find at least one finger's id that is greater than the node key
                if(node.getId().compareTo(finger.getId()) < 0){
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
                for (Finger finger : node.getFingerTable().values()) {
                    if (maxFingerId.compareTo(finger.getId()) < 0){
                        maxFingerId = finger.getId();
                        closestSuccessor = finger;
                    }
                }
            }

            // If the ItemKey is greater than the Node's Id, but the node is already the greatest among fingers, then
            // it should go to the smallest finger
            if(nodeIsGreaterThanAllFingers && itemIsGreaterThanAllFingers && itemKey.compareTo(node.getId()) > 0){
                if(smallestFinger != null)
                    closestSuccessor = smallestFinger;
            }

            //Chord.cLogPrint("queryid: " + itemKey + " minimum distance: " + minimumDistance + " on " + closestSuccessor.getIpAddr() + ":" + closestSuccessor.getPort());

            try {
                // Open socket to chord node
                Socket socket = new Socket(closestSuccessor.getIpAddr(), closestSuccessor.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.PLACE_ITEM + ":" + key.toString() + ":" + item);
                Chord.cLogPrint("Sent: " + Chord.PLACE_ITEM + ":" + key.toString() + ":" + item);

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
            }

            this.node.release();
        }

        return response;
    }

    /**
     * @brief   Control whether the id passed as argument refers to the current node
     * @param   queryId     Finger/Node's identification
     * @return  True if it does refer to the current node, False otherwise
     */
    private boolean doesIdReferToCurrentNode(BigInteger queryId) {
        boolean response = false;

        // If we are working in a nice clockwise direction without wrapping
        if (this.node.getId().compareTo(this.node.getFirstPredecessor().getId()) == 1) {            //TODO: vedere se posso togliere sta roba della clockwise direction
            // If the query id is between our predecessor and us, the query belongs to us
            if ((queryId.compareTo(this.node.getFirstPredecessor().getId()) == 1) && (queryId.compareTo(this.node.getId()) <= 0)) {
                response = true;
            }
        } else { // If we are wrapping
            if ((queryId.compareTo(this.node.getFirstPredecessor().getId()) == 1) || (queryId.compareTo(this.node.getId()) <= 0)) {
                response = true;
            }
        }

        return response;
    }

    /**
     * @brief   Control
     * @param   itemKey
     * @return  True if it does refer to the current node, False otherwise
     */
    private boolean shouldItemBeStoredOnCurrentNode(BigInteger itemKey) {
        boolean response = false;
        boolean noFingerIsBetter = true;
        boolean itemIsGreaterThanAllFingers = true;
        boolean nodeIsSmallerThanAllFingers = true;

        // Look for a node identifier in the finger table that is less than the key id and closest in the ID space to the key id
        for (Finger finger : this.node.getFingerTable().values()) {
            BigInteger distance;

            // Find if any finger id is larger than the item's key and smaller that the current node
            if (finger.getId().compareTo(node.getId()) < 0 && finger.getId().compareTo(itemKey) > 0) {
                noFingerIsBetter = false;
            }

            // Find at least one finger's id that is greater than the item key
            if(itemKey.compareTo(finger.getId()) < 0){
                itemIsGreaterThanAllFingers = false;
            }

            // Find at least one finger id that is smaller than the node's id
            if(finger.getId().compareTo(node.getId()) < 0){
                nodeIsSmallerThanAllFingers = false;
            }
        }

        // If the item's key is between our predecessor and us, the item belongs to us
        if ((itemKey.compareTo(this.node.getId()) <= 0) && noFingerIsBetter){// && !itemIsGreaterThanAllFingers) {
            response = true;
        }
        // If the Item's key is greater than all the fingers ids than the node that should
        // host the item is he one with the smallest id (overflow of the ring)
        else if(itemIsGreaterThanAllFingers && nodeIsSmallerThanAllFingers){
            response = true;
        }

        return response;
    }

    /**
     * @brief   Control whether the id passed as argument refers to the current node's first successor
     * @param   queryId     Finger/Node's identification
     * @return  True if it does refer to the first successor, False otherwise
     */
    private boolean doesIdReferToNextNode(BigInteger queryId) {
        boolean response = false;

        // If we are working in a nice clockwise direction without wrapping
        if (this.node.getId().compareTo(this.node.getFirstSuccessor().getId()) == -1) {
            // If the query id is between our successor and us, the query belongs to our successor
            if ((queryId.compareTo(this.node.getId()) == 1) && (queryId.compareTo(this.node.getFirstSuccessor().getId())<= 0)) {
                response = true;
            }
        } else { // If we are wrapping
            if ((queryId.compareTo(this.node.getId()) == 1) || (queryId.compareTo(this.node.getFirstSuccessor().getId())<= 0)) {
                response = true;
            }
        }

        return response;
    }

    /**
     * @brief   Control whether the id passed as argument refers to the current node's second successor
     * @param   queryId     Finger/Node's identification
     * @return  True if it does refer to the second successor, False otherwise
     */
    private boolean doesIdReferToNextNextNode(BigInteger queryId) {
        boolean response = false;

        // If we are working in a nice clockwise direction without wrapping
        if (this.node.getId().compareTo(this.node.getSecondSuccessor().getId()) == -1) {
            // If the query id is between our successor and us, the query belongs to our successor
            if ((queryId.compareTo(this.node.getId()) == 1) && (queryId.compareTo(this.node.getSecondSuccessor().getId())<= 0)) {
                response = true;
            }
        } else { // If we are wrapping
            if ((queryId.compareTo(this.node.getId()) == 1) || (queryId.compareTo(this.node.getSecondSuccessor().getId())<= 0)) {
                response = true;
            }
        }

        return response;
    }
}
