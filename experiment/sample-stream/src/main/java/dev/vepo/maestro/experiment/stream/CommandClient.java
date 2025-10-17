package dev.vepo.maestro.experiment.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandClient {
    public enum Command {START, STOP, DONE, STATUS};
    private static final Logger logger = LoggerFactory.getLogger(CommandClient.class);
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    
    public void startConnection(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        
        // Read welcome message
        logger.info(in.readLine());
    }
    
    public String sendCommand(Command command, String type, String arguments) throws IOException {
        out.println(String.format("%s:%s,%s", command.name(), type, arguments));
        return in.readLine();
    }

    public String sendCommand(Command command) throws IOException {
        out.println(command.name());
        return in.readLine();
    }
    
    public void stopConnection() throws IOException {
        in.close();
        out.close();
        clientSocket.close();
    }
}