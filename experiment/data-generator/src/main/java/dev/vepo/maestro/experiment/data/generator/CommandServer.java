package dev.vepo.maestro.experiment.data.generator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandServer {
    public static enum Command{ START, STOP, DONE }

    @FunctionalInterface
    public interface Callback {
        void command(Command command);
    }

    private static final Logger logger = LoggerFactory.getLogger(CommandServer.class);

    private final Callback callback;
    private final AtomicBoolean running;
    private ServerSocket serverSocket;

    public CommandServer(Callback callback) {
        this.callback = callback;
        this.running = new AtomicBoolean(false);
    }

    public void start(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        logger.info("Server started on port {}", port);
        
        while (running.get() && !serverSocket.isClosed()) {
            Socket clientSocket = serverSocket.accept();
            processConnection(clientSocket);
        }
        logger.info("Server is done! Closing...");
        stop();
    }
    
    public void stop() throws IOException {
        running.set(false);
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        logger.info("Server stopped");
    }
    
    private void processConnection(Socket clientSocket) {
        try (var out = new PrintWriter(clientSocket.getOutputStream(), true);
                var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            
            String inputLine;
            out.println("SERVER: Ready for commands (START, STOP, DONE)");
            
            while (this.running.get() && (inputLine = in.readLine()) != null) {
                logger.info("Received command: {}", inputLine);
                out.println(processCommand(inputLine));
            }
            
            logger.info("Connection is done!");
        } catch (IOException e) {
            logger.error("Client handler error!", e);
        } finally {
            if (!clientSocket.isClosed()) {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    logger.warn("Cannot close socket!", e);
                }
            }
        }
    }
    
    private String processCommand(String command) {
        return switch (command.toUpperCase()) {
            case "START" -> {
                callback.command(Command.START);
                yield "SERVER: Processing started - Task initiated";
            }
            case "STOP" -> {
                callback.command(Command.STOP);
                yield "SERVER: Processing stopped - Task paused";
            }
            case "DONE" -> {
                callback.command(Command.DONE);
                running.set(false);
                yield "SERVER: Session completed - Goodbye!";
            }
            case "STATUS" -> "SERVER: Server is running: " + running.get();
            default -> "SERVER: Unknown command. Use: START, STOP, DONE, STATUS";
        };
    }
}