package cn.doit08.data_service.controller;

import cn.doit08.data_service.service.AddService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AddController {

    @Autowired
    AddService addService;

    @RequestMapping("/add")
    public int add(int a, int b) {
        int res = addService.addNum(a,b);
        return res;
    }

    @RequestMapping("/person")
    public Person getPerson(){
        Person person = new Person("小新", 23, "男");
        return person;
    }
}
