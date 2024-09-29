package com.example.springkafkapract.vo;

import lombok.Data;

import java.util.ArrayList;

@Data
public class PurchaseLog {
    String orderId; //od-0001
    String userId; //uid-0001
    ArrayList<String> productId; //pq-0001
    String purchaseDt; //20230201080000
    Long price; //23000
}
