import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.*;

public class StreamingJava {
    // Aufgabe 2) a)
    public static <E> Stream<E> flatStreamOf(List<List<E>> list) {
        return list.stream()
                .flatMap(Collection::stream);
    }

    // Aufgabe 2) b)
    public static <E> Stream<E> mergeStreamsOf(Stream<Stream<E>> stream) {
        return stream
                .reduce(Stream.empty(), Stream::concat);
    }

    // Aufgabe 2) c)
    public static <E extends Comparable<? super E>> E minOf(List<List<E>> list) {
        return flatStreamOf(list)
                .toList()
                .parallelStream()
                .min(Comparator.naturalOrder())
                .orElseThrow();
    }

    // Aufgabe 2) d)
    public static <E> E lastWithOf(Stream<E> stream, Predicate<? super E> predicate) {
        return stream
                .filter(predicate)
                .reduce((a,b) -> b)
                .orElse(null);
    }

    // Aufgabe 2) e)
    public static <E> Set<E> findOfCount(Stream<E> stream, int count) {
        Set<E> res = new HashSet<>();
        stream
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .forEach((key, value) -> {if (value == count) res.add(key);} );
        return res;
    }

    // Aufgabe 2) f)
    public static IntStream makeStreamOf(String[] strings) {
        return Arrays
                .stream(strings)
                .flatMapToInt(String::chars);
    }

//-------------------------------------------------------------------------------------------------

    // Aufgabe 3) a)
    public static Stream<String> fileLines(String path) throws IOException {
        return Files
                .newBufferedReader(Paths.get(path))
                .lines()
                .sequential()
                .skip(1)
                .onClose(() -> {System.out.println("Stream closed");}); // Konsolenausgabe bei Schließen von Stream geht nicht
    }

    // Aufgabe 3) b)
    public static double averageCost(Stream<String> lines) {
        double res = lines.flatMap(x -> Stream.of(x.split(",")).skip(12)).mapToDouble(Double::parseDouble).average().orElseThrow();
        System.out.println("3b) " + res);
        return res;
    }

    // Aufgabe 3) c)
    public static long countCleanEnergyLevy(Stream<String> stream) {
        long l = stream.flatMap(x -> Stream.of(x.split(",")).skip(10).limit(1).filter(s -> s.equals(0) || s.equals(""))).count();
        System.out.println("3c) " + l);
        return l;
    }

    // Aufgabe 3) d)
    // TODO:
    //  1. Create record "NaturalGasBilling".
    //  2. Implement static method: "Stream<NaturalGasBilling> orderByInvoiceDateDesc(Stream<String> stream)".
    public static record NaturalGasBilling(
            String InvoiceDate,
            String FromDate,
            String ToDate,
            String BillingDays,
            String BilledGJ,
            String BasicCharge,
            String DeliveryCharges,
            String StorageAndTransport,
            String CommodityCharges,
            String Tax,
            String CleanEnergyLevy,
            String CarbonTax,
            String Amount
    ) {

        public static Stream<NaturalGasBilling> orderByInvoiceDateDesc(Stream<String> stream) {
            System.out.println("\n3d) ");
            return stream.map(x -> new NaturalGasBilling(
                            x.split(",")[0],
                            x.split(",")[1],
                            x.split(",")[2],
                            x.split(",")[3],
                            x.split(",")[4],
                            x.split(",")[5],
                            x.split(",")[6],
                            x.split(",")[7],
                            x.split(",")[8],
                            x.split(",")[9],
                            x.split(",")[10],
                            x.split(",")[11],
                            x.split(",")[12]))
                    .sorted(Comparator.comparing(x -> x.InvoiceDate)); // wie "absteigend" sortieren?
        }

        // Aufgabe 3) e)
        // TODO: Implement object method: "Stream<Byte> toBytes()" for record "NaturalGasBilling".

        public static Stream<Byte> toBytes() throws IOException {
            System.out.println("\n3e) ");
            Stream<String> stream = StreamingJava.fileLines("C:\\Users\\Philipp\\IdeaProjects\\Streaming\\data\\NaturalGasBilling.csv");
            return stream.map(String::getBytes);
            return null;
        }

    }

    // Aufgabe 3) f)
    // TODO: Implement static method: "Stream<Byte> serialize(Stream<NaturalGasBilling> stream)".

    // Aufgabe 3) g)
    // TODO: Implement static method: "Stream<NaturalGasBilling> deserialize(Stream<Byte> stream)".
    // TODO: Execute the call: "deserialize(serialize(orderByInvoiceDateDesc(fileLines(Datei aus f))))"
    // TODO: in a main Method and print the output to the console.

    // Aufgabe 3) h)
    public static Stream<File> findFilesWith(String dir, String startsWith, String endsWith, int maxFiles) {
        // TODO

        return null;
    }

    public static void main(String[] args) throws Exception {

        System.out.println("");

        // 2a)
        List<Integer> e1 = new ArrayList<>();
        e1.add(1);
        e1.add(3);
        List<Integer> e2 = new ArrayList<>();
        e2.add(2);
        e2.add(4);
        List<List<Integer>> e = new ArrayList<>();
        e.add(e1);
        e.add(e2);
        System.out.print("2a) list1: " + e1 + " || list2: " + e2 + " || flatStreamOf: ");
        flatStreamOf(e).forEach(System.out::print);
        System.out.println("");

        // 2b)
        Stream<Integer> s1 = Stream.of(1, 2, 3);
        Stream<Integer> s2 = Stream.of(4, 5, 6);
        System.out.print("2b) stream1: ");
        s1.forEach(System.out::print);
        System.out.print(" || stream2: ");
        s2.forEach(System.out::print);
        System.out.print(" || mergeStreamsOf: ");
        s1 = Stream.of(1, 2, 3);
        s2 = Stream.of(4, 5, 6);
        Stream<Stream<Integer>> s = Stream.of(s1, s2);
        mergeStreamsOf(s).forEach(System.out::print);
        System.out.println("");

        // 2c)
        e1.remove(0);
        e1.add(9);
        System.out.print("2c) list1: " + e1 + " || list2: " + e2 + " || minOf: ");
        System.out.println(minOf(e));

        // 2d)
        s1 = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Predicate<Integer> p = x -> x < 4;
        System.out.print("2d) stream: ");
        s1.forEach(System.out::print);
        System.out.print(" || Predicate: x<4");
        s1 = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9);
        System.out.println(" || lastWithOf: " + lastWithOf(s1, p));

        // 2e)
        s1 = Stream.of(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5);
        System.out.print("2e) stream: ");
        s1.forEach(System.out::print);
        int c = 3;
        System.out.print(" || count: " + c);
        System.out.print(" || findOfCount: ");
        s1 = Stream.of(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5);
        System.out.println(findOfCount(s1, c));

        // 2f)
        String[] sarr = {"test", "Sakshi", "Anas", "Philipp"};
        System.out.print("2f) Array: " + Arrays.toString(sarr) + " || makeStreamOf: ");
        makeStreamOf(sarr).forEach(System.out::print);
        System.out.println();

        // 3a)
        String path = "C:\\Users\\Philipp\\IdeaProjects\\Streaming\\data\\NaturalGasBilling.csv";
        System.out.println("3a) ");
        Stream<String> stream = fileLines(path);

        // 3b)
        stream = fileLines(path);
        averageCost(stream);

        // 3c)
        stream = fileLines(path);
        countCleanEnergyLevy(stream);

        // 3d)
        stream = fileLines(path);
        NaturalGasBilling.orderByInvoiceDateDesc(stream).forEach(System.out::println);

        // 3e)
        stream = fileLines(path);
        Stream<Byte> test = NaturalGasBilling.toBytes();
        test.forEach(System.out::println);

    }
}
