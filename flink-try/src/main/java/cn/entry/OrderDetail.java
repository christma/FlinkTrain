package cn.entry;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;


//orderid int(11) NO 订单id号
//goodsid int(11) NO 商品id号
//prince double(6,2) NO 单价
//num int(11) NO 数量


@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetail {
    private String order_id;
    private String goods_id;
    private String name;
    private Double prince;
    private Integer num;
}
