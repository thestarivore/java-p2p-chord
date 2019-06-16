package com.distribsystems.p2p.chord_lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class Stabilizer extends Thread {
    private Node node;
    private int stabilizationDelay = 0;
    private int pingDelay = 0;

    public Stabilizer(Node node) {
        this.node = node;
    }

    /**
     * @brief   This Method must ensure that each Node's successor pointer is up to date.
     *          It run's periodically the Stability control (once every 8 seconds for now).
     *          The Successor and Predecessor validator and also runs periodically (but with smaller delay, typically 3seconds).
     *
     *          For each node n asks it's successor for the successor's predecessor p, and decides whether p should be n's successor instead (for example if
     *          p has recently joined)
     */
    public void run() {
        while (true) {
            try {
                Socket socket = null;
                PrintWriter socketWriter = null;
                BufferedReader socketReader = null;

                while (true) {
                    // Stabilization Delay
                    Thread.sleep(Chord.STABILIZATION_DELAY * 1000);

                    new Timer().schedule(new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                // Ping Delay
                                Thread.sleep(Chord.PING_DELAY * 1000);

                                // Ping Successors and Predecessors
                                testSuccessor();
                                testPredecessor();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }, 2000);

                    // If it is not ourselves, open a connection to the successor
                    if (!this.node.getIpAddr().equals(this.node.getFirstSuccessor().getIpAddr()) || (this.node.getPort() != this.node.getFirstSuccessor().getPort()))
                    {
                        // Open socket to successor
                        socket = new Socket(this.node.getFirstSuccessor().getIpAddr(), this.node.getFirstSuccessor().getPort());
                        socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

                        // Open reader/writer to chord node
                        socketWriter = new PrintWriter(socket.getOutputStream(), true);
                        socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        // Submit a request for the predecessor -->
                        // n (this node), asks it's successor for the successor's predecessor p, and decides
                        // whether p should be n's successor instead (for example if p has recently joined)
                        socketWriter.println(Chord.REQUEST_PREDECESSOR + ":" + this.node.getId() + " asking " + this.node.getFirstSuccessor().getId());
                        Chord.cLogPrint("Sent: " + Chord.REQUEST_PREDECESSOR + ":" + this.node.getId() + " asking " + this.node.getFirstSuccessor().getId());

                        // Read response from chord
                        String serverResponse = socketReader.readLine();
                        Chord.cLogPrint("Received: " + serverResponse);

                        // Parse server response for address and port
                        String[] predecessorFragments = serverResponse.split(":");
                        String predecessorAddress = predecessorFragments[0];
                        int predecessorPort = Integer.valueOf(predecessorFragments[1]);

                        // If the address:port(of p, the successor's predecessor) that was returned from the server is not ourselves
                        // then we need to adopt it as our new successor
                        try {
                            if (!this.node.getIpAddr().equals(predecessorAddress) || (this.node.getPort() != predecessorPort)) {
                                this.node.acquire();

                                Finger newSuccessor = new Finger(predecessorAddress, predecessorPort);

                                this.node.release();

                                // Close connections
                                socketWriter.close();
                                socketReader.close();
                                socket.close();

                                // Inform new successor that we are now their predecessor
                                socket = new Socket(newSuccessor.getIpAddr(), newSuccessor.getPort());
                                socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

                                // Open writer/reader to new successor node
                                socketWriter = new PrintWriter(socket.getOutputStream(), true);
                                socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                                // Update successor only if connection was successfully
                                // Update finger table entries to reflect new successor
                                this.node.getFingerTable().put(1, this.node.getFingerTable().get(0));
                                this.node.getFingerTable().put(0, newSuccessor);

                                // Update successor entries to reflect new successor
                                this.node.setSecondSuccessor(this.node.getFirstSuccessor());
                                this.node.setFirstSuccessor(newSuccessor);

                                // Tell successor that this node is its new predecessor
                                socketWriter.println(Chord.NEW_PREDECESSOR + ":" + this.node.getIpAddr() + ":" + this.node.getPort());
                                Chord.cLogPrint("Sent: " + Chord.NEW_PREDECESSOR + ":" + this.node.getIpAddr() + ":" + this.node.getPort());
                            }
                        } catch (Exception e){
                            e.printStackTrace();

                            /**
                             * If connection has Failed (maybe because the new successor has left), then reopen the connection with the old successor.
                             * This will give time to the successor to correct it's successors and predecessors so that next Stabilize routine will
                             * give us valid nodes (if any).
                             */

                            // Open socket to successor
                            socket = new Socket(this.node.getFirstSuccessor().getIpAddr(), this.node.getFirstSuccessor().getPort());
                            socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

                            // Open reader/writer to chord node
                            socketWriter = new PrintWriter(socket.getOutputStream(), true);
                            socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        }

                        BigInteger baseTwo = BigInteger.valueOf(2L);

                        this.node.acquire();

                        // Refresh the FingerTable by asking successor for the nodes
                        for (int i = 0; i < Chord.FINGER_TABLE_SIZE; i++) {
                            BigInteger bigResult = baseTwo.pow(i);
                            bigResult = bigResult.add(node.getId());

                            //If overflow occur
                            if (bigResult.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {
                                bigResult = bigResult.subtract(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE));
                            }

                            // Send query to chord
                            socketWriter.println(Chord.FIND_FINGER + ":" + bigResult.longValue());
                            Chord.cLogPrint("Sent: " + Chord.FIND_FINGER + ":" + bigResult.longValue());

                            // Read response from chord
                            serverResponse = socketReader.readLine();

                            // Parse out address and port
                            String[] serverResponseFragments = serverResponse.split(":", 2);
                            if(serverResponseFragments.length == 2) {
                                String[] addressFragments = serverResponseFragments[1].split(":");

                                // Add response finger to table
                                this.node.getFingerTable().put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
                                this.node.setFirstSuccessor(this.node.getFingerTable().get(0));
                                this.node.setSecondSuccessor(this.node.getFingerTable().get(1));

                                Chord.cLogPrint("Received: " + serverResponse);
                            }
                        }

                        this.node.release();

                        // Close connections
                        socketWriter.close();
                        socketReader.close();
                        socket.close();
                    }
                    // Otherwise, if we don't posses a valid successor, open a connection to the predecessor it is not ourselves
                    else if (!this.node.getIpAddr().equals(this.node.getFirstPredecessor().getIpAddr()) || (this.node.getPort() != this.node.getFirstPredecessor().getPort()))
                    {
                        // Open socket to successor
                        socket = new Socket(this.node.getFirstPredecessor().getIpAddr(), this.node.getFirstPredecessor().getPort());
                        socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

                        // Open reader/writer to chord node
                        socketWriter = new PrintWriter(socket.getOutputStream(), true);
                        socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        BigInteger baseTwo = BigInteger.valueOf(2L);

                        this.node.acquire();

                        // Refresh the FingerTable by asking the predecessor for nodes
                        for (int i = 0; i < Chord.FINGER_TABLE_SIZE; i++) {
                            BigInteger bigResult = baseTwo.pow(i);
                            bigResult = bigResult.add(node.getId());

                            //If overflow occur
                            if (bigResult.compareTo(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE)) >= 0) {
                                bigResult = bigResult.subtract(BigInteger.valueOf(2L).pow(Chord.FINGER_TABLE_SIZE));
                            }

                            // Send query to chord
                            socketWriter.println(Chord.FIND_FINGER + ":" + bigResult.longValue());
                            Chord.cLogPrint("Sent: " + Chord.FIND_FINGER + ":" + bigResult.longValue());

                            // Read response from chord
                            String serverResponse = socketReader.readLine();

                            // Parse out address and port
                            String[] serverResponseFragments = serverResponse.split(":", 2);
                            String[] addressFragments = serverResponseFragments[1].split(":");

                            // Add response finger to table
                            this.node.getFingerTable().put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
                            this.node.setFirstSuccessor(this.node.getFingerTable().get(0));
                            this.node.setSecondSuccessor(this.node.getFingerTable().get(1));

                            Chord.cLogPrint("Received: " + serverResponse);
                        }

                        this.node.release();

                        // Close connections
                        socketWriter.close();
                        socketReader.close();
                        socket.close();
                    }

                    //Control if some of the Items from the ItemTable need to be delegated(sent) to another node in the Finger Table
                    checkItemTable();

                    //Print LOGs
                    node.printStatusLogs();
                }
            } catch (InterruptedException e) {
                System.err.println("stabilize thread interrupted");
                e.printStackTrace();
            } catch (UnknownHostException e) {
                System.err.println("stabilize could not find host of first successor");
                e.printStackTrace();
            } catch (IOException e) {
                System.err.println("stabilize could not connect to first successor");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     *  @brief  Check the correctness of the ItemTable by controlling if some of the Items from the ItemTable
     *          need to be delegated(sent) to another node in the Finger Table
     */
    private void checkItemTable() {
        // Iterate all keys in the temp item table, and for each of them control if it needs to be moved
        // to another node
        Map<BigInteger, String> itemTable = new HashMap<>();
        itemTable.putAll(this.node.getItemTable());                 //Iterate Throughout a copy of the ItemTable for concurrency reasons
        for (BigInteger key : itemTable.keySet()) {
            BigInteger baseTwo = BigInteger.valueOf(2L);
            BigInteger ringSize = baseTwo.pow(Chord.FINGER_TABLE_SIZE);
            BigInteger minimumDistance = ringSize;

            node.acquire();

            /**
             * Look for a node identifier in the finger table that is greater(or equal) than the key we are looking for
             * but is also the closer than the current node
             */
            for (Finger finger : node.getFingerTable().values()) {
                BigInteger distance;

                //If Finger Id is smaller than the current node Id, but greater than item's id
                if(finger.getId().compareTo(node.getId()) < 0 && finger.getId().compareTo(key) >= 0){
                    // Then send the Item to the correct Finger node
                    if(placeItem(finger, key)){
                        //Correct Response --> remove the item from this node
                        this.node.getItemTable().remove(key);
                    }
                }
            }

            /**
             * Look for a node identifier in the predecessors and successors that are greater(or equal) than the key we are looking for
             * but is also the closer than the current node
             */
            // FirstSuccessor
            if(node.getFirstSuccessor().getId().compareTo(node.getId()) < 0 && node.getFirstSuccessor().getId().compareTo(key) >= 0){
                // Then send the Item to the correct Finger node
                if(placeItem(node.getFirstSuccessor(), key)){
                    //Correct Response --> remove the item from this node
                    this.node.getItemTable().remove(key);
                }
            }
            // SecondSuccessor
            if(node.getSecondSuccessor().getId().compareTo(node.getId()) < 0 && node.getSecondSuccessor().getId().compareTo(key) >= 0){
                // Then send the Item to the correct Finger node
                if(placeItem(node.getSecondSuccessor(), key)){
                    //Correct Response --> remove the item from this node
                    this.node.getItemTable().remove(key);
                }
            }
            // FirstPredecessor
            if(node.getFirstPredecessor().getId().compareTo(node.getId()) < 0 && node.getFirstPredecessor().getId().compareTo(key) >= 0){
                // Then send the Item to the correct Finger node
                if(placeItem(node.getFirstPredecessor(), key)){
                    //Correct Response --> remove the item from this node
                    this.node.getItemTable().remove(key);
                }
            }
            // SecondPredecessor
            if(node.getSecondPredecessor().getId().compareTo(node.getId()) < 0 && node.getSecondPredecessor().getId().compareTo(key) >= 0){
                // Then send the Item to the correct Finger node
                if(placeItem(node.getSecondPredecessor(), key)){
                    //Correct Response --> remove the item from this node
                    this.node.getItemTable().remove(key);
                }
            }
            node.release();
        }
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
            socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

            // Open reader/writer to chord node
            PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send query to chord
            socketWriter.println(Chord.PLACE_ITEM + ":" + itemKey.toString() + ":" +  node.getItemTable().get(itemKey));
            Chord.cLogPrint("Sent: " + Chord.PLACE_ITEM + ":" + itemKey.toString() + ":" +  node.getItemTable().get(itemKey));

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
     *  @brief Tests that the Successor is still valid, if not it takes action and tries to repair it
     */
    private void testSuccessor() {
        // Only send heartbeats if we are not the destination
        if (!this.node.getIpAddr().equals(this.node.getFirstSuccessor().getIpAddr()) || (this.node.getPort() != this.node.getFirstSuccessor().getPort())) {
            try {
                // Open socket to successor
                Socket socket = new Socket(this.node.getFirstSuccessor().getIpAddr(), this.node.getFirstSuccessor().getPort());
                socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(Chord.PING + ":" + this.node.getId());
                Chord.cLogPrint("Sent: " + Chord.PING + ":" + this.node.getId());

                // Read response
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Received: " + serverResponse);

                // If we do not receive the proper response then something has gone wrong and we need to set our new immediate successor to the backup
                if (!serverResponse.equals(Chord.PONG)) {
                    findNewValidSuccessor();
                }

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                findNewValidSuccessor();
            }
        }
    }

    /**
     *  @brief Tests that the Predecessor is still valid, if not it takes action and tries to repair it
     */
    private void testPredecessor() {
        // Only send heartbeats if we are not the destination
        if (!this.node.getIpAddr().equals(this.node.getFirstPredecessor().getIpAddr()) || (this.node.getPort() != this.node.getFirstPredecessor().getPort())) {
            try {
                // Open socket to predecessor
                Socket socket = new Socket(this.node.getFirstPredecessor().getIpAddr(), this.node.getFirstPredecessor().getPort());
                socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(Chord.PING + ":" + this.node.getId());
                Chord.cLogPrint("Sent: " + Chord.PING + ":" + this.node.getId());

                // Read response
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Received: " + serverResponse);

                // If we do not receive the proper response then something has gone wrong and we need to set our new immediate predecessor to the backup
                if (!serverResponse.equals(Chord.PONG)) {
                    /*this.node.acquire();
                    this.node.setFirstPredecessor(this.node.getSecondPredecessor());
                    this.node.release();*/
                    findNewValidSuccessor();        //TODO: maybe we need to find a valid predecessor instead of successor, or doing nothing at all since the predecessor is not as important
                }

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                /*this.node.acquire();
                this.node.setFirstPredecessor(this.node.getSecondPredecessor());
                this.node.release();*/
                findNewValidSuccessor();        //TODO: maybe we need to find a valid predecessor instead of successor, or doing nothing at all since the predecessor is not as important
            }
        }
    }

    /**
     *  @brief Find at least one New Valid Successor in the FingerTable
     */
    private void findNewValidSuccessor(){
        Chord.cLogPrint("#############################################################################");
        Chord.cLogPrint("THE SUCCESSOR STOPPED RESPONDING. INITIATING RESEARCH OF NEW VALID SUCCESSOR!");
        Chord.cLogPrint("#############################################################################");

        this.node.acquire();

        //new Thread(new Stabilizer(node)).start();
        if(node.getExistingNodeIpAddr() != null)
            node.initFingerTable();
        node.printStatusLogs();

        Finger nextFinger = this.node.getFingerTable().get(0);
        boolean firstSuccesorFound = false;
        boolean secondSuccesorFound = false;
        for (int i = 0; i < Chord.FINGER_TABLE_SIZE; i++) {
            Chord.cLogPrint("## Finger Iteration" + String.valueOf(i) + " ##");
            try {
                nextFinger = node.getFingerTable().get(i);

                // Open socket to successor
                Socket socket = new Socket(nextFinger.getIpAddr(), nextFinger.getPort());
                socket.setSoTimeout(Chord.SOCKET_TIMEOUT*1000);
                Chord.cLogPrint("Try to connect to: " + nextFinger.getIpAddr() + ":" + nextFinger.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(Chord.PING + ":" + this.node.getId());
                Chord.cLogPrint("Sent: " + Chord.PING + ":" + this.node.getId());

                // Read response
                String serverResponse = socketReader.readLine();
                Chord.cLogPrint("Received: " + serverResponse);

                // If we received a PONG in response of the PING
                // We have may have found a new valid successor
                if (serverResponse.equals(Chord.PONG) && (!this.node.getIpAddr().equals(nextFinger.getId()) || this.node.getPort() != nextFinger.getPort())) {
                    if(!firstSuccesorFound) {
                        Chord.cLogPrint("FOUND VALID FIRST SUCCESSOR: " + nextFinger.getIpAddr() + ":" + nextFinger.getPort() + "(" + nextFinger.getId() + ")");
                        this.node.setFirstSuccessor(nextFinger);
                        firstSuccesorFound = true;
                        continue;
                    }else {
                        if(!nextFinger.getIpAddr().equals(this.node.getFirstSuccessor().getIpAddr()) ||
                                nextFinger.getPort() != this.node.getFirstSuccessor().getPort()) {
                            Chord.cLogPrint("FOUND VALID SECOND SUCCESSOR: " + nextFinger.getIpAddr() + ":" + nextFinger.getPort() + "(" + nextFinger.getId() + ")");
                            //this.node.setFirstSuccessor(nextFinger);
                            this.node.setSecondSuccessor(nextFinger);
                            secondSuccesorFound = true;
                            break;
                        }
                    }
                }
                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            }catch (IOException e){

            }
        }

        //If no successor found, try looking into the predecessors
        if(!secondSuccesorFound && !firstSuccesorFound){
            //First Predecessor
            nextFinger = this.node.getFirstPredecessor();
            if (!nextFinger.getIpAddr().equals(this.node.getFirstSuccessor().getIpAddr()) || nextFinger.getPort() != this.node.getFirstSuccessor().getPort()) {
                Chord.cLogPrint("FOUND VALID FIRST SUCCESSOR: " + nextFinger.getIpAddr() + ":" + nextFinger.getPort() + "(" + nextFinger.getId() + ")");
                this.node.setFirstSuccessor(nextFinger);
                firstSuccesorFound = true;
            }
            //Else try with the second Predecessor
            else{
                nextFinger = this.node.getSecondPredecessor();
                if (!nextFinger.getIpAddr().equals(this.node.getFirstSuccessor().getIpAddr()) || nextFinger.getPort() != this.node.getFirstSuccessor().getPort()) {
                    Chord.cLogPrint("FOUND VALID FIRST SUCCESSOR: " + nextFinger.getIpAddr() + ":" + nextFinger.getPort() + "(" + nextFinger.getId() + ")");
                    this.node.setFirstSuccessor(nextFinger);
                    firstSuccesorFound = true;
                }
            }
        }

        //If only found the fist successor, set also the second as the first
        if(!secondSuccesorFound && firstSuccesorFound) {
            this.node.setSecondSuccessor(this.node.getFirstSuccessor());
        }

        this.node.release();
    }
}