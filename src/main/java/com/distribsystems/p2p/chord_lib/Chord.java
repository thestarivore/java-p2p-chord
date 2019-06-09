package com.distribsystems.p2p.chord_lib;

import java.util.Scanner;

public class Chord {
    public final static int         MAXIMUM_FINGER_TABLE_SIZE = 160;
    public final static int         FINGER_TABLE_SIZE = 8;          //Max 160
    public final static int         STABILIZATION_DELAY = 8;        //[seconds]
    public final static int         PING_DELAY = 3;                 //[seconds]
    public static boolean           enableLogs = true;

    public final static String NEW_PREDECESSOR      = "NEW_PREDECESSOR";
    public final static String FIND_FINGER          = "FIND_FINGER";            //FIND_NODE
    public final static String FINGER_FOUND         = "FINGER_FOUND";           //FINGER_FOUND
    public static final String REQUEST_PREDECESSOR  = "REQUEST_PREDECESSOR";
    public final static String PING                 = "PING";
    public final static String PONG                 = "PONG";
    public static final String FIND_ITEM            = "FIND_ITEM";
    public static final String ITEM_FOUND           = "ITEM_FOUND";
    public static final String PLACE_ITEM           = "PLACE_ITEM";
    public static final String ITEM_PLACED          = "ITEM_PLACED";
    public static final String NOT_FOUND            = "NOT_FOUND";

    /**
     * @brief   Log printing function that can be inhibited
     * @param   log String of the log to print
     */
    static public void cLogPrint(String log){
        if(enableLogs)
            System.out.println(log);
    }

    public static boolean isEnableLogs() {
        return enableLogs;
    }

    public static void setEnableLogs(boolean enableLogs) {
        Chord.enableLogs = enableLogs;
    }

    /*public static void main(String[] args){
        String ip = "127.0.0.1";
        int port = 8000;

        /**
         * Program Arguments
         * args[0] = port of the newly created Node
         * args[1] = ip address of the Node to connect to
         * args[2] = port of the Node to connect to
         */
       /* if(args.length > 0){
            port = Integer.parseInt(args[0]);
        }

        if (args.length == 1) {
            // Create new node
            new Node(ip, port);
        } else if (args.length == 3) {
            String existingIpAddress = args[1];
            int existingPort = Integer.parseInt(args[2]);

            // Create new node
            new Node(ip, port, existingIpAddress, existingPort);
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
                new Node(ip, port);
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
                new Node(ip, port, existingIpAddress, existingPort);
            }
        }
    }*/
}
