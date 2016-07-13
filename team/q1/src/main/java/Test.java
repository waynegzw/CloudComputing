import java.math.BigInteger;

/**
 * Created by xgy on 27/02/16.
 */
public class Test {
    private static final BigInteger SECRET_KEY
            = new BigInteger("64266330917908644872330635228106713310880186591609208114244758680898150367880703152525200743234420230");

    public String testMethod() {
        return "hahahah";
    }

    public String waitMethod() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "Finish waiting";
    }

    public static void main(String[] args) {
        Decipher dc = new Decipher();
        Stopwatch time1 = new Stopwatch();
        for (int i = 0; i < 25000; i++) {
            dc.decipher("YNEQREREBJUHTTVATOHATANRYHEBERNBNOYRZCAEOVGYHZNUGYYGBCSCXBPRVNTAHEROBFNYNPNJNVHFZHPFBQNQYCZHEGARPGFB",
                    "29824881671662318316918906016914781134706550990334208872849272858917042444015119314823149633067286528");
        }
        System.out.println(time1.elapsedTime());
        Stopwatch time2 = new Stopwatch();
        for (int i = 0; i < 25000; i++) {
            dc.deCipher("YNEQREREBJUHTTVATOHATANRYHEBERNBNOYRZCAEOVGYHZNUGYYGBCSCXBPRVNTAHEROBFNYNPNJNVHFZHPFBQNQYCZHEGARPGFB",
                    "29824881671662318316918906016914781134706550990334208872849272858917042444015119314823149633067286528");
        }
        System.out.println(time2.elapsedTime());
    }
}
