import java.util.regex.Pattern;

/**
 * Created by Zhangwei on 4/5/16.
 */
public class Main {
    private final static Pattern regex = Pattern.compile(
            "(?i)(https?|ftp):\\/\\/[^\\s/$.?#][^\\s]*" +
                    "|</?ref[^>]*>" +
                    "|'(?![a-zA-Z])|(?<![a-zA-Z])'" +
                    "|[^a-zA-Z'\n]");

    private final static Pattern space = Pattern.compile("\\s{2,}");

    public static void main(String[] args) {
        String s = "'''Anarchism''' is a [[political philosophy]] that advocates [[self-governance|self-governed]] societies with voluntary institutions. These are often described as [[stateless society|stateless societies]],<ref>\"ANARCHISM, a social philosophy that rejects authoritarian government and maintains that voluntary institutions are best suited to express man's natural social tendencies.\" George Woodcock. \"Anarchism\" at The Encyclopedia of Philosophy</ref><ref name=\"iaf-ifa.org\"/>\"In a society developed on these lines, the voluntary associations which already now begin to cover all the fields of human activity would take a still greater extension so as to substitute themselves for the state in all its functions.\" [http://www.theanarchistlibrary.org/HTML/Petr_Kropotkin___Anarchism__from_the_Encyclopaedia_Britannica.html Peter Kropotkin. \"Anarchism\" from the Encyclopædia Britannica]</ref><ref>\"Anarchism.\" The Shorter Routledge Encyclopedia of Philosophy. 2005. p. 14 \"Anarchism is the view that a society without the state, or government, is both possible and desirable.\"</ref> <ref>\"anarchists are opposed to irrational (e.g., illegitimate) authority, in other words, hierarchy — hierarchy being the institutionalisation of authority within a society.\" [http://www.theanarchistlibrary.org/HTML/The_Anarchist_FAQ_Editorial_Collective__An_Anarchist_FAQ__03_17_.html#toc2 \"B.1 Why are anarchists against authority and hierarchy?\"] in [[An Anarchist FAQ]]</ref> ";

        String s1 = "anarchism is a political philosophy that advocates self governance self governed societies with voluntary institutions these are often described as stateless society stateless societies anarchism a social philosophy that rejects authoritarian government and maintains that voluntary institutions are best suited to express man's natural social tendencies george woodcock anarchism at the encyclopedia of philosophy in a society developed on these lines the voluntary associations which already now begin to cover all the fields of human activity would take a still greater extension so as to substitute themselves for the state in all its functions peter kropotkin anarchism from the encyclop dia britannica anarchism the shorter routledge encyclopedia of philosophy p anarchism is the view that a society without the state or government is both possible and desirable anarchists are opposed to irrational e g illegitimate authority in other words hierarchy hierarchy being the institutionalisation of authority within a society b why are anarchists against authority and hierarchy in an anarchist faq";


//        String regex = "([a-z]|((?<=[a-z])'(?=[a-z])))+";
//        s = s.toLowerCase()
//                .replaceAll("(</ref>|<ref.*?>)", " ")
//                .replaceAll("(https?|ftp)://[^\\s/$.?#][^\\s]*", " ")
//                .replaceAll("_", " ");
//        Pattern p = Pattern.compile(regex);
//        Matcher m = p.matcher(s);
//        String result = "";
//        while (m.find()) {
//            result += m.group() + " ";
//        }
        String withMultiSpace = regex.matcher(s).replaceAll(" ").replaceAll("\\s{2,}", " ");
        System.out.println(withMultiSpace.toLowerCase().trim().equals(s1));
        System.out.println(s1.split(" ").length);


    }
}
