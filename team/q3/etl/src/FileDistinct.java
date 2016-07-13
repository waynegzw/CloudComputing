import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by xgy on 22/03/16.
 */
public class FileDistinct {
    public static void main(String[] args) {
        try (Stream<String> stream = Files.lines(Paths.get("/home/xgy/sorted.csv"))) {
            stream
                    .distinct()
                    .forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            List<String> lines = Files.readAllLines(Paths.get("res/nashorn1.js"));
            lines.stream().distinct().forEach(System.out::println);
//            Files.write(Paths.get("res/nashorn1-modified.js"), lines);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}

