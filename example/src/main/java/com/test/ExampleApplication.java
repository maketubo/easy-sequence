package com.test;

import com.maketubo.sequence.action.SequenceAction;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName ExampleApplication
 * @description
 * @date 2020/8/17 22:01
 * @since JDK 1.8
 */
@SpringBootApplication
public class ExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
        System.out.println(SequenceAction.getNext("not_exists", true));
        System.out.println(SequenceAction.getNext("not_exists"));
        System.out.println(SequenceAction.getNext("not_exists"));
        System.out.println(SequenceAction.getNext("not_exists"));
        System.out.println(SequenceAction.getNext("not_exists"));
        System.out.println(SequenceAction.getNext("not_exists"));
    }
}
