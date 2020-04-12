package com.efDemo.ingestion.Java;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class JavaStreamDemo {
    public static void main(String[] args) {
        /*
 [0,1,2,4,5,6,7,4,6,5,4,1,3,5]
偶数 奇数个数
最大值 最小值 平均值

        */

       /* Integer[]arrs =  {0,1,2,4,5,6,7,4,6,5,4,1,3,5};
        Map<Integer, Integer> map = new HashMap<>();
        map = Stream.of(arrs).collect(Collectors.toMap(
                x -> x%2,
                x -> 1));*/

        Integer[] arr={-1,1,2,4,5,6,7,4,6,5,4,1,3,5};
        List list=Stream.of(arr).collect(Collectors.toList());;
        List s1=(List) list.stream().filter(temp->(int)temp%2!=0).sorted().collect(Collectors.toList());
        List s2=(List) list.stream().filter(temp->(int)temp%2==0).sorted().collect(Collectors.toList());
        System.out.println(s1+"\n"+s2);


        System.out.println(Arrays.stream(new int[]{-1,1,2,4,5,6,7,4,6,5,4,1,3,5})
                .map(x -> x % 2)
                .count());

        System.out.println(Arrays.stream(new int[]{-1,1,2,4,5,6,7,4,6,5,4,1,3,5})
                .filter(x -> x % 2 != 0)
                .count());


        Integer max  = Stream.of(-1,1,2,4,5,6,7,4,6,5,4,1,3,5)
                .reduce(Integer.MIN_VALUE, (Integer a, Integer b) -> a>b?a:b);
        System.out.println("max numbert = " + max);

        Integer min = Stream.of(-1,1,2,4,5,6,7,4,6,5,4,1,3,5)
                .reduce(Integer.MAX_VALUE, (Integer a, Integer b) -> a>b?b:a);
        System.out.println("min numbert = " + min);



        Integer min2 = Stream.of(-1,1,2,4,5,6,7,4,6,5,4,1,3,5)
                .reduce(Integer.MAX_VALUE, (Integer a, Integer b) -> Math.min(a,b));
        System.out.println("min numbert = " + min2);



        System.out.println(Arrays.stream(new int[]{-1,1,2,4,5,6,7,4,6,5,4,1,3,5}).max());

        System.out.println(Arrays.stream(new int[]{-1,1,2,4,5,6,7,4,6,5,4,1,3,5}).min());


        Arrays.stream(arr).reduce(Integer::max).ifPresent(System.out::println);
        Arrays.stream(arr).reduce(Integer::min).ifPresent(System.out::println);

    }
}
