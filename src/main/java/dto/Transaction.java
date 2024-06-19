package dto;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Transaction {
    //    {"customerId": "nicolegordon", "transactionId": "b93bf288-dd19-4746-94a4-a783ac001e78", "transactionDate": "2024-06-18T20:17:14.158494+0000", "produ
    //        ctId": "product2", "productName": "Smartphone", "productCategory": "Electronics", "productPrice": 717.1, "productQuantity": 5, "productBrand": "Samsung",
    //        "currency": "USD", "paymentMethod": "online_transfer", "totalAmount": 3585.5}
    private String customerId;
    private String transactionId;
    private Timestamp transactionDate;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private String productBrand;
    private String currency;
    private String paymentMethod;
    private double totalAmount;
}
