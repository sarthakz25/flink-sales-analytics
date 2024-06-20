package dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;

@Data
@AllArgsConstructor
public class SalesPerCategory {
    private Date transaction_date;
    private String category;
    private Double totalSales;
}
