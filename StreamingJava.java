import java.io.File;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StreamingJava {
    // Aufgabe 2) a)
    public static <E> Stream<E> flatStreamOf(List<List<E>> list) {
        return list.stream().flatMap(Collection::stream);
    }

    // Aufgabe 2) b)
    public static <E> Stream<E> mergeStreamsOf(Stream<Stream<E>> stream) {
        return stream.reduce(Stream.empty(), Stream::concat);
    }

    // Aufgabe 2) c)
    public static <E extends Comparable<? super E>> E minOf(List<List<E>> list) throws Exception {
        List<E> stream = flatStreamOf(list).toList();
        Optional<E> res = stream.parallelStream().min(Comparator.naturalOrder());
        if (res.isEmpty()) {
            throw new Exception();
        }
        return res.get();
    }

    // Aufgabe 2) d)
    public static <E> E lastWithOf(Stream<E> stream, Predicate<? super E> predicate) {
        // TODO

        return null;
    }

    // Aufgabe 2) e)
    public static <E> Set<E> findOfCount(Stream<E> stream, int count) {
        // TODO

        return null;
    }

    // Aufgabe 2) f)
    public static IntStream makeStreamOf(String[] strings) {
        // TODO

        return null;
    }

//-------------------------------------------------------------------------------------------------

    // Aufgabe 3) a)
    public static Stream<String> fileLines(String path) {
        // TODO

        return null;
    }

    // Aufgabe 3) b)
    public static double averageCost(Stream<String> lines) {
        // TODO

        return 0d;
    }

    // Aufgabe 3) c)
    public static long countCleanEnergyLevy(Stream<String> stream) {
        // TODO

        return 0L;
    }

    // Aufgabe 3) d)
    // TODO:
    //  1. Create record "NaturalGasBilling".
    //  2. Implement static method: "Stream<NaturalGasBilling> orderByInvoiceDateDesc(Stream<String> stream)".

    // Aufgabe 3) e)
    // TODO: Implement object method: "Stream<Byte> toBytes()" for record "NaturalGasBilling".

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
        System.out.print("list1: " + e1 + " || list2: " + e2 + " || flatStreamOf: ");
        flatStreamOf(e).forEach(System.out::print);
        System.out.println("");

        // 2b)
        Stream<Integer> s1 = Stream.of(1,2,3);
        Stream<Integer> s2 = Stream.of(4,5,6);
        System.out.print("stream1: ");
        s1.forEach(System.out::print);
        System.out.print(" || stream2: ");
        s2.forEach(System.out::print);
        System.out.print(" || mergeStreamsOf: ");
        s1 = Stream.of(1,2,3);
        s2 = Stream.of(4,5,6);
        Stream<Stream<Integer>> s = Stream.of(s1,s2);
        mergeStreamsOf(s).forEach(System.out::print);
        System.out.println("");

        // 2c)
        e1.remove(0);
        e1.add(9);
        System.out.print("list1: " + e1 + " || list2: " + e2 + " || minOf: ");
        System.out.println(minOf(e));

        System.out.println("");
    }
}
