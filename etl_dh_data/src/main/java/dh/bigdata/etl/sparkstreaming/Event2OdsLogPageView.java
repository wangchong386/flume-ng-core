package dh.bigdata.etl.sparkstreaming;
 
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogPageView implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_pageview中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {
        if (event == null)
            System.out.println("helloworld");
        String type = (event.getTags().get("type") == null) ? "" : event.getTags().get("type").toString();
        String url = event.getU();
        String uuid = (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString();
        return RowFactory.create(event.getName(),
                event.getId(),
                event.getVid(),
                event.getUsrid(),
                event.getSid(),
                event.getVt(),
                event.getIp(),
                event.getUa(),
                event.getF(),
                event.getD(),
                event.getRefurl(),
                event.getUlevel(),
                event.getAid(),
                event.getU(),
                event.getCou(),
                PigConv.getUuid(type,url,uuid),
                (event.getTags().get("pt") == null) ? "" : event.getTags().get("pt").toString(),
                (event.getTags().get("lpid") == null) ? "" : event.getTags().get("lpid").toString(),
                (event.getTags().get("activity") == null) ? "" : event.getTags().get("activity").toString(),
                (event.getTags().get("supplierid") == null) ? "" : event.getTags().get("supplierid").toString(),
                (event.getTags().get("pic") == null) ? "" : event.getTags().get("pic").toString(),
                (event.getTags().get("clkloc") == null) ? "" : event.getTags().get("clkloc").toString(),
                (event.getTags().get("type") == null) ? "" : event.getTags().get("type").toString(),
                (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                (event.getTags().get("catepubid") == null) ? "" : event.getTags().get("catepubid").toString(),
                (event.getTags().get("picl") == null) ? "" : event.getTags().get("picl").toString(),
                (event.getTags().get("pril") == null) ? "" : event.getTags().get("pril").toString(),
                (event.getTags().get("pnl") == null) ? "" : event.getTags().get("pnl").toString(),
                (event.getTags().get("spcst") == null) ? "" : event.getTags().get("spcst").toString(),
                (event.getTags().get("wdc") == null) ? "" : event.getTags().get("wdc").toString(),
                (event.getTags().get("rnl") == null) ? "" : event.getTags().get("rnl").toString(),
                (event.getTags().get("ttp") == null) ? "" : event.getTags().get("ttp").toString(),
                (event.getTags().get("imploc") == null) ? "" : event.getTags().get("imploc").toString(),
                PigConv.getSite(event),
                PigConv.getLang(event),
                (event.getTags().get("ordlang") == null) ? "" : event.getTags().get("ordlang").toString(),
                (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                (event.getTags().get("version") == null) ? "" : event.getTags().get("version").toString(),
                (event.getTags().get("lastvisittime") == null) ? "" : event.getTags().get("lastvisittime").toString(),
                (event.getTags().get("pvn") == null) ? "" : event.getTags().get("pvn").toString(),
                (event.getTags().get("vnum") == null) ? "" : event.getTags().get("vnum").toString(),
                (event.getTags().get("pagedur") == null) ? "" : event.getTags().get("pagedur").toString(),
                (event.getTags().get("session") == null) ? "" : event.getTags().get("session").toString(),
                (event.getTags().get("subpt") == null) ? "" : event.getTags().get("subpt").toString(),
                event.getTags().get("currentDate").toString(),
                event.getName());
    }
}
