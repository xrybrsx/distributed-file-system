import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

public class Dstore {
    private static Socket socketCPORT;
    private static PrintWriter outCPORT;
    private static BufferedReader inCPORT;
    private final LinkedList<File> files = new LinkedList<>();
    private int cport;
    private int port;
    private int timeout;
    private String folderPath;
    private File folder;
    private ServerSocket serverSocket;
    private ServerSocket clientServer;

    public Dstore() throws IOException {
    }

    //main creates a Dstore
    public static void main(String[] args) throws IOException {
        Dstore store = new Dstore();
        store.setPort(Integer.parseInt(args[0]));
        store.setCport(Integer.parseInt(args[1]));
        store.setTimeout(Integer.parseInt(args[2]));
        store.setFolder(new File(args[3]));
//        store.setCport(12345);
//        store.setTimeout(1000);
//        store.setPort((int) Math.round(Math.random() * 100));
//        File folder = new File("Folder " + store.toString());
//        store.setFolder(folder);
        store.setFolderPath(store.getFolder().getAbsolutePath());
        if (!store.getFolder().isDirectory()){
        store.getFolder().mkdir();}
        for (String s : store.getFolder().list()) {
          File ff = new File(store.getFolder().getAbsolutePath() + "/" + s);
          store.getFiles().add(ff);
        }
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, store.getPort());

        //connecting with the controller
        System.out.println("[DStore " + store.toString() + "] Connecting...");
        socketCPORT = new Socket("localhost", store.getCport());
        outCPORT = new PrintWriter(socketCPORT.getOutputStream(), true);
        inCPORT = new BufferedReader(new InputStreamReader(socketCPORT.getInputStream()));
        System.out.println("[DStore] My port is " + store.getPort());

        //sending JOIN operation
        outCPORT.println("JOIN " + store.getPort());
        DstoreLogger.getInstance().messageSent(socketCPORT,"JOIN " + store.getPort());
        DstoreLogger.getInstance().messageReceived(socketCPORT,inCPORT.readLine());

        //telling the controller of any already existing files in the dstore's database
        String list = "";
        for (int i = 0; i < store.getFiles().size(); i++) {
            list = list + " "+ store.getFiles().get(i).getName();
        }
        outCPORT.println("LIST" + list);
        DstoreLogger.getInstance().messageSent(socketCPORT, "LIST" + list);
        store.start();

    }

    /*function for listening.
    *any new connection to the server is sent to a separate thread.
    *threads terminate if there is no more a client connected.*/
    public void start() throws IOException {
        serverSocket = new ServerSocket(getPort());
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> {
                try {
                    receive(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    //key function that deals with different requests
    private void receive(Socket socket) throws IOException {
        Socket s = socket;
        BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        boolean exit = false;
        try {
                String line;
                if ((line = in.readLine()) != null) {
                    DstoreLogger.getInstance().messageReceived(socket, line);
                    String[] words = line.split(" ");
                    System.out.println(line);
                    switch (words[0]) {
                        case "STORE":
                            System.out.println("[DStore] STORE_IN_PROGRESS");
                            if (!getFiles().contains(words[1])){ //if the file is not in the database, store it
                            store(line, s, out);}
                            else {out.println("ERROR_FILE_ALREADY_EXISTS");}
                            break;
                        case "LOAD_DATA":
                            System.out.println("[DStore] LOAD_IN_PROGRESS");
                            //checking if the file exists in database, else close the socket
                            boolean exists = false;
                            for (File f : getFiles()) {
                                if (f.getName().equals(words[1])){
                                    exists = true;}}
                                if (exists) {
                                    load(line, s);
                                    System.out.println("[DStore] LOAD_COMPLETED");
                                } else {
                                    socket.close();
                                    System.out.println("[DStore] NO_SUCH_FILE");
                                }

                            break;
                        case "REMOVE":
                            System.out.println("[DStore] REMOVE_IN_PROGRESS");
                            File file = new File(line.split(" ")[1]);
                            remove(file);
                            break;
                        case "LIST":
                            System.out.println("[DStore] LIST_IN_PROGRESS");
                            String list = "";
                            for (int i = 0; i < getFiles().size(); i++) {
                                list = list + " "+ getFiles().get(i).getName();
                            }
                            out.println("LIST" + list);
                            DstoreLogger.getInstance().messageSent(socket, "LIST" + list);
                            break;
                        default:
                            out.println("UNRECOGNIZED!");
                            DstoreLogger.getInstance().messageSent(socket, "UNRECOGNIZED!");
                            break;
                    }
                }

        } catch (IOException e) {
            System.err.println("[DStore] CLIENT DISCONNECTED");
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                System.err.println("[DStore] CONNECTION FAIL");
            }
            out.close();
        }
    }


    public LinkedList<File> getFiles() {
        return files;
    }

    public ServerSocket getClientServer() {
        return clientServer;
    }

    public void setClientServer(ServerSocket clientServer) {
        this.clientServer = clientServer;
    }

    public Socket getSocket() {
        return socketCPORT;
    }

    public void setSocket(Socket socket) {
        Dstore.socketCPORT = socket;
    }


    public void connect() throws IOException {

    }


    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public void setServerSocket(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    public int getCport() {
        return cport;
    }

    public void setCport(int cport) {
        this.cport = cport;
    }

    public File getFolder() {
        return folder;
    }

    public void setFolder(File folder) {
        this.folder = folder;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getFolderPath() {
        return folderPath;
    }

    public void setFolderPath(String fileName) {
        this.folderPath = fileName;
    }

    //remove a file from database
    public void remove(File file) {
        if (getFolder().list() != null) {
            for (File f : getFolder().listFiles()) {
                // if there is such file in database, it is deleted
                if ((f.getName()).equals(file.getName())) {
                    boolean deleted = f.delete();

                    //let controller know the file is deleted
                    if (deleted) {
                        outCPORT.println("REMOVE_ACK " + f.getName());
                        DstoreLogger.getInstance().messageSent(socketCPORT, "REMOVE_ACK " + f.getName());
                    }

                }

            }
        }
    }

    //simple loading function
    public boolean load(String line, Socket s) throws IOException {
        boolean success = false;
        FileInputStream fos = null;
        BufferedOutputStream bos = null;
        OutputStream outStream = null;
        System.out.println("[DStore] LOADING");

        File file = null;
        for (File f : getFiles()) {
            if (f.getName().equals(line.split(" ")[1])) {
                file = f;
            }
        }
        if (file != null) {
            try {
                fos = new FileInputStream(file);
                outStream = s.getOutputStream();
                byte[] buf = new byte[(int) file.length()];
                int buflen;
                while ((buflen = fos.read(buf)) != -1) {
                    System.out.println("*");
                    outStream.write(buf,0,buflen);
                    success = true;

                }
            }catch(Exception e){
                System.out.println("error" + e);
            }finally {
                {if (fos != null) {
                    try {
                        fos.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                }
            }
        }

        return success;
    }
    public boolean store(String line, Socket s, PrintWriter out) throws IOException {
        System.out.println("[DStore] STORING");

        int bytesRead;
        FileOutputStream fos = null;
        boolean success = false;
        PrintWriter writer = null;
        Socket socket = null;

        try {
            socket = s;
            writer = out;

            File file = new File(getFolderPath() + "/" + line.split(" ")[1]);
            int size = Integer.parseInt(line.split(" ")[2]);
            System.out.println(file.getAbsoluteFile());
            if (!fileExists(file)) {
                writer.println("ACK");
                DstoreLogger.getInstance().messageSent(socket,"ACK");
                byte[] buf = new byte[size];
                InputStream inStream = socket.getInputStream();
                bytesRead = inStream.readNBytes(buf, 0, buf.length);
                fos = new FileOutputStream(file);
                while (bytesRead != -1) {
                    System.out.println("*");
                    fos.write(buf, 0, bytesRead);
                    bytesRead = inStream.read(buf);
                }
                inStream.close();
                success = true;
                System.out.println("[DStore] File " + file
                        + " downloaded (" + buf.length + " bytes read)");
                addFile(file);

                //letting controller know the file is stored
                outCPORT.println("STORE_ACK " + file.getName());
                DstoreLogger.getInstance().messageSent(socketCPORT,"STORE_ACK " + file.getName());
            } else System.err.println("FILE_EXISTS");
        } catch (IOException e) {

            e.printStackTrace();
        } finally {

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return success;
    }

    public Boolean fileExists(File file) {
        Boolean exists = false;
        for (File f : files) {
            if (f.getName().equals(file.getName())) {
                exists = true;
            }
        }
        return exists;
    }

    public void addFile(File file) {
        files.add(file);
    }
}
