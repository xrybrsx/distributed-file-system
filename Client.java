import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client {
    private static int  cport = 12345;
    FileInputStream fis = null;
    BufferedInputStream bis = null;
    OutputStream os = null;
    Socket ss = null;
    public void connect() throws IOException {
        Socket socket = new Socket("localhost", cport);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println("LIST");
        System.out.println(in.readLine());
        try {
//            ---STORE----

            out.println("STORE Clipboard01.pdf 15207");
            String resp = in.readLine();
            System.out.println(resp);
            Socket ss = new Socket("localhost", ((Integer.parseInt(resp.split(" ")[1]))));
            PrintWriter out1 = new PrintWriter(ss.getOutputStream(), true);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(ss.getInputStream()));
            out1.println("STORE Clipboard01.pdf 15207");
            System.out.println(in1.readLine());

            File myFile = new File("/Users/xrybrsx/Documents/dfs-cw/Clipboard01.pdf");

            try {
                FileInputStream fin = new FileInputStream(myFile);
                OutputStream outStream = ss.getOutputStream();
                byte[] buf = new byte[15207];
                int buflen;
                while ((buflen = fin.read(buf)) != -1) {
                    System.out.print("*");
                    outStream.write(buf, 0, buflen);
                }
            } catch (Exception e) {
                System.out.println("error" + e);
            }
//            BufferedInputStream bin = new BufferedInputStream(fin);
//            bin.read(mybytearray,0,mybytearray.length);
//            os = ss.getOutputStream();
//            System.out.println("Sending " + myFile.getName() + "(" + mybytearray.length + " bytes)");
//            os.write(mybytearray,0,mybytearray.length);
//            os.flush();
            System.out.println("Done.");
            in1.close();
            out1.close();
            ss.close();
            System.out.println(in.readLine());
        } catch (Exception e) {
            System.out.println("error" + e);}
//            System.out.println(in.readLine());
////            System.out.println(in.readLine());
////            while ((server = in.readLine()) != null) {
////                System.out.println(server);
////                str = keyboard.readLine();
////                if (str != null){
////                    out.println(str);
////                }
////            }
//        }
//        catch(Exception e){System.out.println("error "+e);}

            //            ---REMOVE----
//         out.println("REMOVE Clipboard01.pdf");
//         System.out.println(in.readLine());

        // -----LOAD-----

        out.println("LOAD Clipboard01.pdf");
        String line;
        System.out.println(line = in.readLine());
        Socket ss = new Socket("localhost",((Integer.parseInt(line.split(" ")[1]))));
            PrintWriter out1 = new PrintWriter(ss.getOutputStream(),true);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(ss.getInputStream()));
            out1.println("LOAD_DATA Clipboard01.pdf");int bytesRead;
       File file = new File("Clipboard01.pdf");
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        byte[] buf = new byte[Integer.parseInt(line.split(" ")[2])];
        InputStream inStream = ss.getInputStream();
        bytesRead = inStream.readNBytes(buf, 0, buf.length);
        fos = new FileOutputStream(file);
        while (bytesRead != -1) {
            System.out.print("*");
            fos.write(buf, 0, bytesRead);
            bytesRead = inStream.read(buf);
        }
        inStream.close();
        fos.close();
        ss.close();

               //     ---REMOVE----
         out.println("REMOVE Clipboard01.pdf");
         System.out.println(in.readLine());



        out.println("LIST");
        System.out.println(in.readLine());
    }
    public static void main(String[] args) throws IOException {
//        Path currentRelativePath = Paths.get("");
//        File file = new File("");
//        String ss = file.getAbsolutePath().toString();
//        String s = currentRelativePath.toAbsolutePath().toString();
//        System.out.println("Current relative path is: " + s + " || " + ss );\
        Client client = new Client();
        client.connect();
    }
}
