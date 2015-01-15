package edu.caltech.nanodb.client;


import java.io.IOException;

import edu.caltech.nanodb.server.NanoDBServer;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.Command;


/**
 * This class is used for starting the NanoDB database in exclusive mode, where
 * only a single client interacts directly with the database system.
 */
public class ExclusiveClient extends InteractiveClient {
    private static Logger logger = Logger.getLogger(ExclusiveClient.class);


    public static final boolean FLUSH_DATA_AFTER_CMD = true;


    private NanoDBServer server;


    public void startup() {
        // Start up the various database subsystems that require initialization.
        server = new NanoDBServer();
        try {
            server.startup();
        }
        catch (IOException e) {
            System.out.println("DATABASE STARTUP FAILED:");
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }


    public void handleCommand(Command cmd) {
        server.doCommand(cmd, false);

        /* TODO:  Re-enable?
        if (FLUSH_DATA_AFTER_CMD)
            StorageManager.getInstance().flushAllData();
         */
    }


    public void shutdown() {
        // Shut down the various database subsystems that require cleanup.
        if (!server.shutdown())
            System.out.println("DATABASE SHUTDOWN FAILED.");
    }


    public static void main(String args[]) {
        ExclusiveClient client = new ExclusiveClient();

        client.startup();
        client.mainloop();
        client.shutdown();
    }
}

