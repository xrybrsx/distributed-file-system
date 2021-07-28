import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private static int cport;
    private static int R;
    //list of the index entries
    private static Vector<Entry> entries = new Vector();
    //string of files known to be stored in dstores
    private static Vector<String> files = new Vector();
    //mapping dstore public server ports with their local port used for connecting with the controller
    private static Map<Integer, Socket> DStoreSockets = new HashMap<>();
    private static Map<String, Integer> filesMap = new HashMap<>();
    //list of only the public server ports of dstores (for convenience)
    private static Vector publicPorts = new Vector();
    private int timeout;
    private int rebalance_period;
    private ServerSocket serverSocket;
    private Socket s;
    private BufferedReader in;
    private PrintWriter out;

    public Controller() throws IOException {
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        serverSocket = new ServerSocket(cport);
    }

    public static Vector<Entry> getEntries() {
        return entries;
    }

    public static void setEntries(Vector<Entry> entries) {
        Controller.entries = entries;
    }

    public static Vector<String> getFiles() {
        return files;
    }

    public static void setFiles(Vector<String> files) {
        Controller.files = files;
    }

    public static Map<String, Integer> getFilesMap() {
        return filesMap;
    }

    public static void setFilesMap(Map<String, Integer> filesMap) {
        Controller.filesMap = filesMap;
    }

    public static Map<Integer, Socket> getDStoreSockets() {
        return DStoreSockets;
    }

    public static void setDStoreSockets(Map<Integer, Socket> DStoreSockets) {
        Controller.DStoreSockets = DStoreSockets;
    }

    public static Vector getPublicPorts() {
        return publicPorts;
    }

    public static void setPublicPorts(Vector publicPorts) {
        Controller.publicPorts = publicPorts;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Controller controller = null;

//        try {
//            controller = new Controller();
//            controller.setR(1);
//            controller.setTimeout(1000);
//            controller.setRebalance_period(10);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        if (args.length == 4) {
            try {
                controller = new Controller();
                controller.setCport(Integer.parseInt(args[0]));
                controller.setR(Integer.parseInt(args[1]));
                controller.setTimeout(Integer.parseInt(args[2]));
                controller.setRebalance_period(Integer.parseInt(args[3]));
                System.out.println(controller);
                controller.start();
            } catch (NumberFormatException e) {
                System.err.println("Argument" + args[0] + " must be an integer.");
                System.exit(1);
            }
        }
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public void setServerSocket(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    public Socket getS() {
        return s;
    }

    public void setS(Socket s) {
        this.s = s;
    }

    public BufferedReader getIn() {
        return in;
    }

    public void setIn(BufferedReader in) {
        this.in = in;
    }

    public PrintWriter getOut() {
        return out;
    }

    public void setOut(PrintWriter out) {
        this.out = out;
    }

    public void stop() throws IOException {
        serverSocket.close();
    }

    public void close(Socket socket, BufferedReader in, PrintWriter out) throws IOException {
        socket.close();
        in.close();
        out.close();
    }

    /*function for listening.
     *any new connection to the server is sent to a separate thread.
     *threads terminate if there is no more a client connected.*/
    public void start() throws IOException, ClassNotFoundException {
        ServerSocket server = new ServerSocket(12345);
        while (true) {
            Socket s = server.accept();
//
            new Thread(() -> {
                try {
                    receive(s);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
            System.out.println("[Controller] Connected");
        }
    }

    private int toInt(String s) {
        return Integer.parseInt(s);
    }

    //key function that deals with different requests
    private void receive(Socket s) throws IOException {
        Socket socket = s;
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        boolean exit = false;
        try {
            while (!exit) {
                String line;
                if ((line = in.readLine()) != null) {
                    ControllerLogger.getInstance().messageReceived(socket, line);
                    String[] words = line.split(" ");
                    switch (words[0]) {
                        case "JOIN":
                            //adding the dstore to controller's lists
                            getDStoreSockets().put(toInt(words[1]), socket);
                            getPublicPorts().add(toInt(words[1]));
                            //checking if there are files already on the dstore's database
                            //and adding those files to the index entries if they exist
                            out.println("LIST");
                            ControllerLogger.getInstance().messageSent(socket, "LIST");
                            String list_files = in.readLine();
                            ControllerLogger.getInstance().messageReceived(socket, list_files);
                            String[] filesList = list_files.split(" ");
                            if (filesList.length > 1) {
                                for (int i = 1; i < filesList.length; i++) {
                                    if (!files.contains(filesList[i])) {
                                        files.add(filesList[i]);
                                        Entry e = new Entry();
                                        e.setStatus("COMPLETED");
                                        e.setFileName(filesList[i]);
                                        e.getDStorePorts().add(socket.getPort());

                                    }
                                }
                            }
                            break;
                        case "LIST":
                            System.out.println("[Controller] LIST in progress");
                            String list = list();
                            out.println(list);
                            ControllerLogger.getInstance().messageSent(socket, list);
                            break;
                        case "STORE":
                            System.out.println("[Controller] STORE in progress");
                            if (files.contains(line.split(" ")[1])) {
                                out.println("ERROR_FILE_ALREADY_EXISTS");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_ALREADY_EXISTS");
                            } else if (R > getDStoreSockets().size()) {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_NOT_ENOUGH_DSTORES");
                            } else {
                                String p = store(line);
                                out.println(p);
                                ControllerLogger.getInstance().messageSent(socket, p);
                                if (p.split(" ")[0].equals("STORE_TO")) {
                                    //creating index entry for the file
                                    String ports = p.substring(9);
                                    String[] prts = ports.split(" ");
                                    Entry i = new Entry();
                                    i.setFileName(words[1]);
                                    AtomicInteger ai = new AtomicInteger(toInt(words[2]));
                                    i.setFilesize(ai);
                                    for (int j = 0; j < prts.length; j++) {
                                        i.getDStorePorts().add(getDStoreSockets().get(Integer.parseInt(prts[j])).getPort());
                                        i.getWorkingStores().add(getDStoreSockets().get(Integer.parseInt(prts[j])).getPort());
                                        getDStoreSockets().get(Integer.parseInt(prts[j])).setSoTimeout(timeout);
                                    }

                                    i.setClientSocket(socket);
                                    i.checkStatus();
                                    System.out.println(i.getClientSocket() + " " + i.getDStorePorts() + " " + i.getFilesize() + " " + i.getFileName() + " " + i.getStatus());
                                    getEntries().add(i);
                                }
                            }
                            break;
                        case "STORE_ACK":
                            String file = words[1];

                            for (Entry e : getEntries())
                                if (e.getFileName().equals(file)) {
                                    e.getAcks().add(socket.getPort());
                                    socket.setSoTimeout(0);
                                    e.checkStatus();
                                    if (e.getStatus().equals("COMPLETED")) {
                                        if (!files.contains(file)) {
                                            files.add(file);
                                        }
                                        Socket ss = e.getClientSocket();
                                        PrintWriter outt = new PrintWriter(ss.getOutputStream(), true);
                                        outt.println("STORE_COMPLETE");
                                        ControllerLogger.getInstance().messageSent(ss, "STORE_COMPLETE");
                                        e.getAcks().clear();
                                    }
                                }
                            break;
                        case "LOAD":
                            System.out.println("[Controller] LOAD in progress");
                            String filefile = words[1];
                            if (R > getDStoreSockets().size()) {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_NOT_ENOUGH_DSTORES");
                            } else if (files.contains(words[1])) {
                                for (Entry e : getEntries()) {
                                    if (e.getFileName().equals(filefile)) {
                                        if (e.getDStorePorts() != null) {
                                            int localport = (int) e.getDStorePorts().get(0);
                                            for (Map.Entry<Integer, Socket> entry : getDStoreSockets().entrySet()) {
                                                if (entry.getValue().getPort() == localport) {
                                                    int port = entry.getKey();
                                                    out.println("LOAD_FROM " + port + " " + e.getFilesize());
                                                    ControllerLogger.getInstance().messageSent(socket, "LOAD_FROM " + port + " " + e.getFilesize());

                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                            }
                            break;
                        case "RELOAD":

                            System.out.println("[Controller] RELOAD in progress");
                            String file1 = words[1];
                            if (files.contains(words[1])) {
                                for (Entry e : getEntries()) {
                                    if (e.getFileName().equals(file1)) {
                                        if (e.getWorkingStores().size() > 1) {
                                            e.getWorkingStores().remove(0);
                                            int localport = e.getWorkingStores().get(0);
                                            for (Map.Entry<Integer, Socket> entry : getDStoreSockets().entrySet()) {
                                                if (entry.getValue().getPort() == localport) {
                                                    int port = entry.getKey();
                                                    out.println("LOAD_FROM " + port + " " + e.getFilesize());
                                                    ControllerLogger.getInstance().messageSent(socket, "LOAD_FROM " + port + " " + e.getFilesize());

                                                }
                                            }
                                        } else {
                                            out.println("ERROR_LOAD");
                                            ControllerLogger.getInstance().messageSent(socket, "ERROR_LOAD");
                                        }
                                    }
                                }
                            } else {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                            }
                            break;
                        case "REMOVE":
                            System.out.println("[Controller] REMOVE in progress");
                            File filee = new File(line.split(" ")[1]);
                            if (R > getDStoreSockets().size()) {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_NOT_ENOUGH_DSTORES");
                            } else if (!files.contains(filee.getName())) {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                            } else {
                                for (Entry e : getEntries()) {
                                    if (e.getFileName().equals(filee.getName())) {
                                        if (e.getDStorePorts() != null) {
                                            for (int i = 0; i < e.getDStorePorts().size(); i++) {
                                                int localport = (int) e.getDStorePorts().get(i);
                                                for (Map.Entry<Integer, Socket> entry : getDStoreSockets().entrySet()) {
                                                    if (entry.getValue().getPort() == localport) {
                                                        getDStoreSockets().get(entry.getKey()).setSoTimeout(timeout);
                                                        Socket socketStore = new Socket("localhost", entry.getKey());
                                                        new PrintWriter(socketStore.getOutputStream(), true).println("REMOVE " + filee.getName());
                                                        // out_remove.println("REMOVE " + filee.getName());
                                                        ControllerLogger.getInstance().messageSent(socketStore, "REMOVE " + filee.getName());

                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            break;
                        case "REMOVE_ACK":
                            String fileRemove = words[1];
                            for (Entry e : getEntries()) {
                                if (e.getFileName().equals(fileRemove)) {
                                    e.getAcks().add(socket.getPort());
                                    e.checkStatus();
                                    socket.setSoTimeout(0);
                                    if (e.getStatus().equals("COMPLETED")) {
                                        files.remove(fileRemove);
                                        Socket ss = e.getClientSocket();
                                        PrintWriter outt = new PrintWriter(ss.getOutputStream(), true);
                                        outt.println("REMOVE_COMPLETE");
                                        ControllerLogger.getInstance().messageSent(socket, "REMOVE_COMPLETE");
                                    }
                                    e.getAcks().clear();
                                }
                            }


                            break;
                        default:
                            out.println("UNRECOGNISED");
                            System.err.println("UNRECOGNISED");
                            break;
                    }
                } else {
                    exit = true;
                }
            }
        } catch (IOException e) {
            System.err.println("[Controller] CLIENT DISCONNECTED");

        } finally {
            try {
                in.close();
            } catch (IOException e) {
                System.err.println("[Controller] CONNECTION FAIL");
            }
            out.close();
        }
    }


    public String list() {
        String s = "";
        if (files != null) {
            for (int i = 0; i < files.size(); i++) {
                s = s + " " + files.get(i);
            }
        } else return "LIST";
        return "LIST" + s;
    }

    public String store(String s) {
        if (DStoreSockets.size() < R) {
            return "ERROR_NOT_ENOUGH_DSTORES";
        } else {
            String ports = "";
            for (int i = 0; i < R; i++) {
                ports = ports + " " + publicPorts.get(i);
            }
            return "STORE_TO" + ports;
        }
    }

    public int getCport() {
        return cport;
    }

    public void setCport(int cport) {
        Controller.cport = cport;
    }

    public int getR() {
        return R;
    }

    public void setR(int r) {
        R = r;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getRebalance_period() {
        return rebalance_period;
    }

    public void setRebalance_period(int rebalance_period) {
        this.rebalance_period = rebalance_period;
    }

    public void STORE(File file) {
        String file_name = file.getName();

    }

    //Nested class for Index entries for the files stored
    public static class Entry {
        private File file;
        private String fileName;
        private AtomicInteger filesize;
        //list of dstores storing the file
        private Vector<Integer> DStorePorts = new Vector();
        //the client currently storing the file
        //it's used so that the client can be notified when the storing is complete
        private Socket clientSocket;
        private String status;
        //list of dstores which have already finished storing the file
        private Vector acks = new Vector();
        private Vector<Integer> workingStores = new Vector();

        public Entry() {

        }

        public Vector<Integer> getWorkingStores() {
            return workingStores;
        }

        public void setWorkingStores(Vector<Integer> workingStores) {
            this.workingStores = workingStores;
        }

        public File getFile() {
            return file;
        }

        public void setFile(File file) {
            this.file = file;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public AtomicInteger getFilesize() {
            return filesize;
        }

        public void setFilesize(AtomicInteger filesize) {
            this.filesize = filesize;
        }

        public Vector getDStorePorts() {
            return DStorePorts;
        }

        public void setDStorePorts(Vector DStorePorts) {
            this.DStorePorts = DStorePorts;
        }

        public Socket getClientSocket() {
            return clientSocket;
        }

        public void setClientSocket(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Vector getAcks() {
            return acks;
        }

        public void setAcks(Vector acks) {
            this.acks = acks;
        }

        //setting the file to a COMPLETED status if all dstores have sent an acknowledgment for finishing storing
        private void checkStatus() {

            if (getDStorePorts().equals(getAcks())) {
                setStatus("COMPLETED");
            } else setStatus("IN_PROGRESS");
        }
    }

}
