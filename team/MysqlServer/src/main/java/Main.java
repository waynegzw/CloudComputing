import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
	// write your code here
        try {
            PrintStream out2 = new PrintStream(System.out, true, "UTF-8");
            String hah = "@#юл.";
            System.out.println(Arrays.toString(hah.getBytes()));
            System.out.println(Charset.defaultCharset());
            System.out.println(3%4);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
