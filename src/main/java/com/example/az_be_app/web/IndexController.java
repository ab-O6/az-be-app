package com.example.az_be_app.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
public class IndexController {

    @GetMapping("/")
    public @ResponseBody String helloWorld() {

        log.trace("IndexController >> helloWorld()");
        log.debug("IndexController >> helloWorld()");
        log.info("IndexController >> helloWorld()");
        log.warn("IndexController >> helloWorld()");
        log.error("IndexController >> helloWorld()");

        return "Hello World";
    }
}
