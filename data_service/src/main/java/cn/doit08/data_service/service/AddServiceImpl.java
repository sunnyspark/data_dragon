package cn.doit08.data_service.service;

import org.springframework.stereotype.Service;

@Service
public class AddServiceImpl implements AddService {
    @Override
    public int addNum(int a, int b) {
        return a + b;
    }
}
