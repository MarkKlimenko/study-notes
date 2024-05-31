package core;

import java.util.ArrayList;
import java.util.List;

public class Generics {
    List<Integer> list = new ArrayList<>();
    List<Number> listNumber = new ArrayList<>();

    {
        doNumber(list); // NOT COMPILE
        doWildcard(list);
        doUpperBoundedWildcard(list);
        doLowerBoundedWildcard(list);
        doLowerBoundedWildcard(listNumber);
    }

    static void doNumber(List<Number> list) {
        list.forEach(System.out::println);
    }

    static void doWildcard(List<?> list) {
        list.forEach(System.out::println);
        Object i = list.get(0);
        Integer j = list.get(0);
        list.add(12);
        list.add(12L);
        list.add(new Object());
    }

    static void doUpperBoundedWildcard(List<? extends Integer> list) {
        list.forEach(System.out::println);
        Object i = list.get(0);
        Integer j = list.get(0);
        list.add(12);
        list.add(12L);
        list.add(new Object());
    }

    static void doLowerBoundedWildcard(List<? super Integer> list) {
        list.forEach(System.out::println);
        Object i = list.get(0);
        Integer j = list.get(0);
        list.add(12);
        list.add(12L);
        list.add(new Object());
    }

    static void doIt(List<?> list) {
        Integer j = list.get(0);
        list.add(123);
    }

    static void doIt(List<? extends Integer> list) {
        Integer j = list.get(0);
        list.add(123);
    }

    static void doIt(List<? super Integer> list) {
        Integer j = list.get(0);
        list.add(123);
    }

    public static void main(String[] args) {

    }
}
