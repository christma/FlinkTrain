package cn.entry;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

//订单
//id int(11)NO 是 id
//uid int(11)NO 会员id号
//addtime int(11)NO 购买时间

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private String uid;
    private String order_id;
    private Long add_time;
}
