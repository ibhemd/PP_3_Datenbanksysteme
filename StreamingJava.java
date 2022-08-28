import javax.xml.crypto.Data;
import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
        long l = stream.flatMap(x -> Stream.of(x.split(",")).skip(10).limit(1).filter(s -> s.equals("0") || s.equals(""))).count();
        System.out.println("3c) " + l);
        return l;
    }

    // Aufgabe 3) d)
    // TODO:
    //  1. Create record "NaturalGasBilling".
    //  2. Implement static method: "Stream<NaturalGasBilling> orderByInvoiceDateDesc(Stream<String> stream)".
    public record NaturalGasBilling(
            Date InvoiceDate,
            Date FromDate,
            Date ToDate,
            int BillingDays,
            double BilledGJ,
            double BasicCharge,
            double DeliveryCharges,
            double StorageAndTransport,
            double CommodityCharges,
            double Tax,
            double CleanEnergyLevy,
            double CarbonTax,
            double Amount
    ) {

        public static Stream<NaturalGasBilling> orderByInvoiceDateDesc(Stream<String> stream) {
            return stream.map(x -> {
                double cleanEnergyLevy = 0L;
                if (!x.split(",")[10].equals("")) {
                    cleanEnergyLevy = Double.parseDouble(x.split(",")[10]);
                }
                try {
                    return new NaturalGasBilling(
                        new SimpleDateFormat("yyyy-MM-dd").parse(x.split(",")[0]),
                        new SimpleDateFormat("yyyy-MM-dd").parse(x.split(",")[1]),
                        new SimpleDateFormat("yyyy-MM-dd").parse(x.split(",")[2]),
                        Integer.parseInt(x.split(",")[3]),
                        Double.parseDouble(x.split(",")[4]),
                        Double.parseDouble(x.split(",")[5]),
                        Double.parseDouble(x.split(",")[6]),
                        Double.parseDouble(x.split(",")[7]),
                        Double.parseDouble(x.split(",")[8]),
                        Double.parseDouble(x.split(",")[9]),
                        cleanEnergyLevy,
                        Double.parseDouble(x.split(",")[11]),
                        Double.parseDouble(x.split(",")[12]));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return null;
            }).sorted((a,b) -> b.InvoiceDate.compareTo(a.InvoiceDate));
        }

        // Aufgabe 3) e)
        // TODO: Implement object method: "Stream<Byte> toBytes()" for record "NaturalGasBilling".
        public Stream<Byte> toBytes() throws IOException {
            return Stream.of(
                    "\n",
                    this.InvoiceDate.getYear()+1900, "-",
                    String.format("%02d", this.InvoiceDate.getMonth()+1), "-",
                    this.InvoiceDate.getDate(), ",",
                    this.FromDate.getYear()+1900, "-",
                    String.format("%02d", this.FromDate.getMonth()+1), "-",
                    this.FromDate.getDate(), ",",
                    this.ToDate.getYear()+1900, "-",
                    String.format("%02d", this.ToDate.getMonth()+1), "-",
                    this.ToDate.getDate(), ",",
                    this.BillingDays, ",",
                    this.BilledGJ, ",",
                    this.BasicCharge, ",",
                    this.DeliveryCharges, ",",
                    this.StorageAndTransport, ",",
                    this.CommodityCharges, ",",
                    this.Tax, ",",
                    this.CleanEnergyLevy, ",",
                    this.CarbonTax, ",",
                    this.Amount)
                    .map(Object::toString)
                    .map(String::chars)
                    .flatMap(x -> x.mapToObj(y -> (byte) y));
        }

        // Aufgabe 3) f)
        // TODO: Implement static method: "Stream<Byte> serialize(Stream<NaturalGasBilling> stream)".
        public static Stream<Byte> serialize(Stream<NaturalGasBilling> stream) {
            return stream.flatMap(x -> {
                try {
                    return x.toBytes();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            });
        }

        // Aufgabe 3) g)
        // TODO: Implement static method: "Stream<NaturalGasBilling> deserialize(Stream<Byte> stream)".
        // TODO: Execute the call: "deserialize(serialize(orderByInvoiceDateDesc(fileLines(Datei aus f))))"
        // TODO: in a main Method and print the output to the console.
        public static Stream<NaturalGasBilling> deserialize(Stream<Byte> stream) {
            List<Byte> bytelist = new ArrayList<>();
            String s = "";

            // Byte -> byte -> char -> füge zu String hinzu
            stream.forEach(bytelist::add);
            for(Byte b : bytelist) {
                s = s + (char) (byte) b;
            }

            // teile String bei Zeilenumbruch
            String[] stringarr = s.split("\n");
            // lösche 1. Element, da leere Zeile
            stringarr = Arrays.copyOfRange(stringarr, 1, stringarr.length-1);

            // String[] -> List<String>
            List<String> stringlist = Arrays.stream(stringarr).toList();

            //List<String> -> Stream<String>
            Stream<String> stringstream = stringlist.stream();

            //Stream<String> -> Stream<NaturalGasBilling> (durch Aufruf von "orderByInvoiceDate")
            return NaturalGasBilling.orderByInvoiceDateDesc(stringstream);
        }

    }

    // Aufgabe 3) h)
    public static Stream<File> findFilesWith(String dir, String startsWith, String endsWith, int maxFiles) throws IOException {
        return Files.walk(Paths.get(dir))
                .filter(x -> x.getFileName().toString().startsWith(startsWith))
                .filter(x -> x.getFileName().toString().endsWith(endsWith))
                .limit(maxFiles)
                .map(x -> new File(x.toString()));
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
        String path = "data\\NaturalGasBilling.csv";
        System.out.println("3a) ");
        Stream<String> stream = fileLines(path);

        // 3b)
        stream = fileLines(path);
        averageCost(stream);

        // 3c)
        stream = fileLines(path);
        countCleanEnergyLevy(stream);

        // 3d) // wie Comparator.comparing ???
        System.out.println("\n3d)");
        stream = fileLines(path);
        NaturalGasBilling.orderByInvoiceDateDesc(stream).forEach(System.out::println);

        // 3e)
        System.out.println("\n3e)");
        stream = fileLines(path);
        NaturalGasBilling.orderByInvoiceDateDesc(stream).forEach(x -> {
            try {
                x.toBytes()
                .forEach(System.out::print);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });

        // 3f)
        System.out.println("\n\n3f)");

        DataOutputStream dos_stm = new DataOutputStream(new FileOutputStream("data\\NaturalGasBillingNew.csv"));

        Stream<String> outstringstream = fileLines("data\\NaturalGasBilling.csv");
        Stream<NaturalGasBilling> outnaturalgasbillingstream = NaturalGasBilling.orderByInvoiceDateDesc(outstringstream);
        Stream<Byte> outbytestream = NaturalGasBilling.serialize(outnaturalgasbillingstream);

        outbytestream.forEach(System.out::print);

        outstringstream = fileLines("data\\NaturalGasBilling.csv");
        outnaturalgasbillingstream = NaturalGasBilling.orderByInvoiceDateDesc(outstringstream);
        outbytestream = NaturalGasBilling.serialize(outnaturalgasbillingstream);

        String headers = "Invoice Date,From Date,To Date,Billing Days,Billed GJ,Basic charge,Delivery charges,Storage and transport,Commodity charges,Tax,Clean energy levy,Carbon tax,Amount";
        byte[] headerbytes = headers.getBytes(StandardCharsets.UTF_8);
        dos_stm.write(headerbytes);

        outbytestream.forEach(x -> {
            try {
                dos_stm.writeByte((int) x);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });

        // 3g)
        System.out.println("\n\n3g)");
        NaturalGasBilling.deserialize(NaturalGasBilling.serialize(NaturalGasBilling.orderByInvoiceDateDesc(fileLines("data\\NaturalGasBillingNew.csv")))).forEach(System.out::println);

        // 3h)
        System.out.println("\n3h)");
        findFilesWith("data\\", "N", ".csv", 1).forEach(System.out::println);
    }
}
